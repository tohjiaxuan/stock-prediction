from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from airflow.utils.task_group import TaskGroup

import json
import os
import pandas as pd
import pandas_ta as ta

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bq_client = bigquery.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'


def build_transform_taskgroup(dag: DAG) -> TaskGroup:
    transform_taskgroup = TaskGroup(group_id = 'transform_taskgroup')

    ############################
    # Define Python Functions  #
    ############################
    def if_f_stock_exists():
        try:
            metadata = bq_client.dataset(DWH_DATASET)
            table_ref = metadata.table('F_STOCKS')
            bq_client.get_table(table_ref)
            return True
        except:
            return False
    
    def query_stock_dwh():
        bq_client = bigquery.Client()
        query = """SELECT Date, Open, High, Low, Close, Volume, Dividends, Stock_Splits, Stock
        FROM `stockprediction-344203.stock_prediction_datawarehouse.F_STOCKS` ORDER BY `Date` DESC LIMIT 6000"""
        df = bq_client.query(query).to_dataframe()

        return df

    def helper_sma_prices(input_df):
        # Obtain unique values
        uniq_stocks = input_df['Stock'].unique()
        
        sma_stocks = []
        # Check for null values, if present, do ffill
        for ticker in uniq_stocks:
            print("Currently handling", ticker)
            curr_df = input_df.loc[input_df['Stock'] == ticker]

            # Sort by date
            curr_df = curr_df.sort_values(by = ['Date'])
            curr_df = curr_df.reset_index(drop = True)

            # Forward fill values
            if (curr_df.isnull().values.any() == True):
                curr_df = curr_df.fillna(method='ffill')
                    
            # Conduct TA analysis
            curr_df['SMA_50'] = ta.sma(curr_df['Close'], length = 50, append=True)
            curr_df['SMA_200'] = ta.sma(curr_df['Close'], length=200, append=True)
            curr_df["GC"] = ta.sma(curr_df['Close'], length = 50, append=True) > ta.sma(curr_df['Close'], length=200, append=True)
            curr_df["DC"] = ta.sma(curr_df['Close'], length = 50, append=True) > ta.sma(curr_df['Close'], length=200, append=True)
            sma_stocks.append(curr_df)
        
        # Transform to df
        final_df = pd.concat(sma_stocks, ignore_index = True)

        # Add Constant column for fact table later on
        final_df['Price Category'] = "Stock"
            
        print("Transforming Stocks Data Complete")
        return final_df

    def query_stage_table():
        query = "select * from `stockprediction-344203.stock_prediction_staging_dataset.distinct_hist_stock_prices`"
        stage_df = bq_client.query(query).to_dataframe()

        return stage_df

    def update_sma():
        new_df = query_stage_table()
        old_df = query_stock_dwh()

        df = old_df.append(new_df, ignore_index = True)

        uniq_dates = new_df['Date'].unique()
        output = helper_sma_prices(df)
        output = output[output['Date'].isin(uniq_dates)]
        return output

    def sma_prices():
        check_dwh = if_f_stock_exists()
        if check_dwh:
            result = update_sma()
        else:
            temp = query_stage_table()
            result = helper_sma_prices(temp)
        print(result)
        result.to_parquet('gs://stock_prediction_is3107/final_stock.parquet', engine='pyarrow', index=False)
        
    ############################
    # Define Airflow Operators #
    ############################

    #############################
    # Transformation in Staging #
    #############################
    # Remove Duplicates exchange rate
    distinct_exchange = BigQueryOperator(
        task_id = 'distinct_exchange_task',
        use_legacy_sql = False,
        sql = f'''
                create or replace table `{PROJECT_ID}.{STAGING_DATASET}.distinct_exchange_rate`
                as select distinct parse_timestamp('%Y-%m-%d', end_of_day) as Date,
                concat(temp.end_of_day, '-EXR') as EXR_ID, *
                except (
                    end_of_day, preliminary, timestamp
                ),
                preliminary as ex_rate_preliminary, timestamp as ex_rate_timestamp
                from `{PROJECT_ID}.{STAGING_DATASET}.init_exchange_rates` as temp
        ''',
        dag = dag,
    )

    # Reformat and remove duplicates interest rate
    distinct_interest = BigQueryOperator(
        task_id = 'distinct_interest_task',
        use_legacy_sql = False,
        sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.distinct_interest_rate` 
        as select distinct parse_timestamp('%Y-%m-%d', end_of_day) as Date, 
        concat(temp.end_of_day, '-INR') as INR_ID, *
        except(
            end_of_day, preliminary, timestamp,
            interbank_overnight, interbank_1w, interbank_1m, interbank_2m, interbank_3m,
            interbank_6m, interbank_12m, commercial_bills_3m, usd_sibor_3m, sgs_repo_overnight_rate
        ),
        preliminary as int_rate_preliminary, timestamp as int_rate_timestamp
        from `{PROJECT_ID}.{STAGING_DATASET}.init_interest_rates` as temp
        ''',
        dag = dag
    )

    # Remove Duplicates
    distinct_stock_prices = BigQueryOperator(
        task_id = 'distinct_stock_prices_task',
        use_legacy_sql = False,
        sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.distinct_hist_stock_prices` 
        as select distinct *
        from `{PROJECT_ID}.{STAGING_DATASET}.init_hist_stock_prices`
        ''',
        dag = dag
    )

    # Add SMA to df
    sma_stock = PythonOperator(
        task_id = 'sma_stock_task',
        python_callable = sma_prices,
        dag = dag
    )

    # Load sma data from GCS to BQ
    load_sma = GoogleCloudStorageToBigQueryOperator(
        task_id = 'stage_sma_task',
        bucket = 'stock_prediction_is3107',
        source_objects = ['final_stock.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.final_hist_prices',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    ############################
    # Define Tasks Hierarchy   #
    ############################
    [distinct_exchange, distinct_interest, distinct_stock_prices] 
    distinct_stock_prices >> sma_stock >> load_sma
    [distinct_exchange, distinct_interest, load_sma] 

    return transform_taskgroup