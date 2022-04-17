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
import logging
import os
import pandas as pd
import pandas_ta as ta

logging.basicConfig(level=logging.INFO)

# Establish connection with staging tables in DWH
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bq_client = bigquery.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'

def build_transform_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup for transformation of data in BigQuery staging tables

    Parameters
    ----------
    dag: An airflow DAG

    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    transform_taskgroup = TaskGroup(group_id = 'transform_taskgroup')

    ############################
    # Define Python Functions  #
    ############################

    def if_f_stock_exists():
        """Check if Stocks Fact Table is present in DWHS

        Returns
        -------
        bool
        """
        try:
            # Establish connection with BigQuery
            bq_client = bigquery.Client()
            query = 'SELECT COUNT(`Date`) FROM `stockprediction-344203.stock_prediction_datawarehouse.F_STOCKS`'
            
            # Convert queried result to df
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return True
            else:
                return False
        except:
            return False
    
    def query_stock_dwh():
        """Query stocks data from dwh for technical analysis

        Returns
        -------
        dataframe
            Contains the historical stocks data for the past 200 days for 30 stocks
        """
        # Establish connection with DWH
        bq_client = bigquery.Client()
        
        # Query 6000 rows because we require 200 days worth of past data for each stock
        # Value can change depending on the number of days for SMA / stock (30*200)
        query = """SELECT Date, Open, High, Low, Close, Volume, Dividends, Stock_Splits, Stock
        FROM `stockprediction-344203.stock_prediction_datawarehouse.F_STOCKS`  WHERE `Price_Category` = 'Stock' ORDER BY `Date` DESC LIMIT 6000"""
        df = bq_client.query(query).to_dataframe()
        return df

    def helper_sma_prices(input_df):
        """Helper function to obtain the sma and golden, death crosess for each stock
        
        Parameters
        ----------
        input_df: dataframe
            Historical stock prices

        Returns
        -------
        dataframe
            Contains historical prices, sma50, sma200, golden and death crosses
        """
        # Obtain unique values
        uniq_stocks = input_df['Stock'].unique()
        
        sma_stocks = []
        # Check for null values, if present, do ffill
        for ticker in uniq_stocks:
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
            curr_df["DC"] = ta.sma(curr_df['Close'], length = 50, append=True) < ta.sma(curr_df['Close'], length=200, append=True)
            sma_stocks.append(curr_df)
        
        # Transform to df
        final_df = pd.concat(sma_stocks, ignore_index = True)

        # Add Constant column for fact table later on
        final_df['Price Category'] = "Stock"
            
        logging.info('Completed technical analysis for stocks')
        return final_df

    def query_stage_table():
        """Query stocks staging table

        Returns
        -------
        dataframe
            Contains the historical stocks data from staging tables
        """
        query = "select * from `stockprediction-344203.stock_prediction_staging_dataset.distinct_hist_stock_prices`"
        stage_df = bq_client.query(query).to_dataframe()
        logging.info('Queried from stocks staging table')

        return stage_df
    
    def query_int_stage():
        """Query interest rate staging table

        Returns
        -------
        dataframe
            Contains the historical interest rate data from staging tables
        """
        bq_client = bigquery.Client()
        query = """SELECT * FROM `stockprediction-344203.stock_prediction_staging_dataset.distinct_interest_rate`
        ORDER BY `Date` DESC"""
        df = bq_client.query(query).to_dataframe()
        logging.info('Queried from interest rate staging table')

        # To prepare df for lagging dates, change column name
        df.rename(columns={'Date':'Actual Date'}, inplace=True)

        return df
    
    def query_ex_stage():
        """Query exchange rate staging table

        Returns
        -------
        dataframe
            Contains the historical exchnage rate data from staging tables
        """
        bq_client = bigquery.Client()
        query = """SELECT `Date` FROM `stockprediction-344203.stock_prediction_staging_dataset.distinct_exchange_rate`
        ORDER BY `Date` DESC"""
        df = bq_client.query(query).to_dataframe()
        logging.info('Queried from ex rate staging table')

        return df

    def update_sma():
        """Helpder function to obtain sma and golden, death crosess for updated stock data

        Returns
        -------
        dataframe
            Contains historical prices, sma50, sma200, golden and death crosses for updated stock
        """
        new_df = query_stage_table()
        old_df = query_stock_dwh()

        # Create new dataframe that contains the past 200 days of data and the newly updated stock data
        df = old_df.append(new_df, ignore_index = True)

        # Get unique dates to prevent duplicated data
        uniq_dates = new_df['Date'].unique()
        output = helper_sma_prices(df) # Calculate SMA, golden and death crosses
        output = output[output['Date'].isin(uniq_dates)] # Only keep records that are present in updated stock data
        return output

    def sma_prices():
        """Obtain sma and golden, death crosess for stock data
        """
        check_dwh = if_f_stock_exists() # Check if fact table exists
        # If it exists, then we just have to update SMA instead of populating for everying
        if check_dwh:
            logging.info('Start to update SMA, GC and DC for updated stock data')
            result = update_sma()
            logging.info('Completed SMA, GC and DC update')
        else:
            logging.info('Start to initialise SMA, GC and DC')
            temp = query_stage_table()
            result = helper_sma_prices(temp)
            logging.info('Completed SMA, GC and DC initialisation')
        
        # Send to GCS cloud
        logging.info("Sent final stock data to GCS")
        result.to_parquet('gs://stock_prediction_is3107/final_stock.parquet', engine='pyarrow', index=False)
    
    def lag_int():
        """Obtain lagged date for interest rate
        """
        int_df = query_int_stage() 
        ex_df = query_ex_stage() # Lagged dates are obtained from exchange rate

        # To ensure index is the same
        int_df = int_df.reset_index(drop=True)
        ex_df = ex_df.reset_index(drop=True)

        dates =  ex_df['Date'].to_list()
        print(int_df)
        
        # If interest date has more data, we start from first row
        if (len(ex_df) < len(int_df)):
            int_df = int_df.iloc[1:]
        
        lag_int = int_df
        lag_int['Date'] = dates

        # Rename columns to ensure consistency
        if 'date' in lag_int.columns and 'inr_id' in lag_int.columns:
            lag_int.rename(columns={'date': 'Date', 'inr_id': 'INR_ID'}, inplace=True)    

        logging.info("Sent lagged date interest rate data to GCS")
        lag_int.to_parquet('gs://stock_prediction_is3107/lag_interest.parquet', engine='pyarrow', index=False)

    def if_d_commodities_exists():
        """Check if Commodities Dimension Table is present in DWHS

        Returns
        -------
        bool
        """
        try:
            bq_client = bigquery.Client()
            query = 'SELECT COUNT(`Date`) FROM `stockprediction-344203.stock_prediction_datawarehouse.D_COMMODITIES`'
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return True
            else:
                return False
        except:
            return False

    def query_commodities_dwh():
        """Query commodities dwh table

        Returns
        -------
        dataframe
            Contains the historical commodities data from staging tables
        """
        bq_client = bigquery.Client()
        query = """SELECT *
        FROM `stockprediction-344203.stock_prediction_datawarehouse.D_COMMODITIES` ORDER BY `Date` DESC"""
        df = bq_client.query(query).to_dataframe()

        return df

    def query_commodities_table():
        """Query gold, silver, crude oil staging tables

        Returns
        -------
        dataframe
            3 dataframes, gold, silver and crude oil respectivel
        """
        gold_query = "SELECT DISTINCT * FROM `stockprediction-344203.stock_prediction_staging_dataset.distinct_gold`"
        gold_stage_df = bq_client.query(gold_query).to_dataframe()

        silver_query = "SELECT DISTINCT * FROM `stockprediction-344203.stock_prediction_staging_dataset.distinct_silver`"
        silver_stage_df = bq_client.query(silver_query).to_dataframe()

        crude_oil_query = "SELECT DISTINCT * FROM `stockprediction-344203.stock_prediction_staging_dataset.distinct_crude_oil`"
        crude_oil_stage_df = bq_client.query(crude_oil_query).to_dataframe()

        return gold_stage_df, silver_stage_df, crude_oil_stage_df

    def consolidate_commodities(gold_df, silver_df, crude_oil_df):
        """Combines gold, silver and crude oil into 1 table with Categories

        Returns
        -------
        dataframe
            Contains gold, silver and cruide oil into 1 df
        """
        # Remove duplicates
        gold_df.drop_duplicates(inplace=True)
        silver_df.drop_duplicates(inplace=True)
        crude_oil_df.drop_duplicates(inplace=True)

        # Add Constant column for fact table later on
        logging.info('Adding price categories')
        gold_df['Price Category'] = "Gold"
        silver_df['Price Category'] = "Silver"
        crude_oil_df['Price Category'] = "Crude Oil"

        # Concatenate all rows
        commodities = []
        commodities.append(gold_df)
        commodities.append(silver_df)
        commodities.append(crude_oil_df)
        final_df = pd.concat(commodities, ignore_index = True)

        # Sort by date and price category
        final_df = final_df.sort_values(by = ['Date', 'Price Category'])
        final_df = final_df.reset_index(drop = True)
        logging.info('Combined gold, silver and crude oil into commodities')
        return final_df

    def transform_commodities():
        """Transform commodities as required

        Returns
        -------
        dataframe
            Contains gold, silver and cruide oil into 1 df
        """
        logging.info('Start to query from staging tables for gold, silver and crude oil')
        gold_df, silver_df, crude_oil_df = query_commodities_table()
        final_df = consolidate_commodities(gold_df, silver_df, crude_oil_df)
        return final_df

    def update_commodities(new_df):
        """Transform commodities as required

        Returns
        -------
        dataframe
            Contains gold, silver and cruide oil into 1 df with updated data
        """
        commodities_temp = []
        old_df = query_commodities_dwh()
        old_df.rename({'Price_Category': 'Price Category'}, axis=1, inplace=True)
        commodities_temp.append(old_df)
        commodities_temp.append(new_df)
        updated_df = pd.concat(commodities_temp, ignore_index = True)
        updated_df.drop_duplicates(inplace=True)
        return updated_df

    def commodities():
        """Create commodities table
        """
        logging.info('Start to transform commodities table')
        df = transform_commodities()
        check_dwh = if_d_commodities_exists() 
        if check_dwh:
            logging.info('Commodities table (Update)')
            result = update_commodities(df)
        else:
            result = df
        logging.info('Send commodities table to GCS bucket')
        result.to_parquet('gs://stock_prediction_is3107/final_commodities.parquet', engine='pyarrow', index=False)
  
    ############################
    # Define Airflow Operators #
    ############################

    #############################
    # Transformation in Staging #
    #############################
    # Remove duplicates and cast data types exchange rate
    distinct_exchange = BigQueryOperator(
        task_id = 'distinct_exchange_task',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.distinct_exchange_rate`
                AS SELECT DISTINCT PARSE_TIMESTAMP('%Y-%m-%d', end_of_day) AS Date,
                CONCAT(temp.end_of_day, '-EXR') AS EXR_ID,
                CAST(eur_sgd AS STRING) AS eur_sgd, CAST(gbp_sgd AS STRING) AS gbp_sgd,
                CAST(usd_sgd AS STRING) AS usd_sgd, CAST(aud_sgd AS STRING) AS aud_sgd,
                CAST(cad_sgd AS STRING) AS cad_sgd, CAST(cny_sgd_100 AS STRING) AS cny_sgd_100,
                CAST(hkd_sgd_100 AS STRING) AS hkd_sgd_100, CAST(inr_sgd_100 AS STRING) AS inr_sgd_100,
                CAST(idr_sgd_100 AS STRING) AS idr_sgd_100, CAST(jpy_sgd_100 AS STRING) AS jpy_sgd_100,
                CAST(krw_sgd_100 AS STRING) AS krw_sgd_100, CAST(myr_sgd_100 AS STRING) AS myr_sgd_100,
                CAST(twd_sgd_100 AS STRING) AS twd_sgd_100, CAST(nzd_sgd AS STRING) AS nzd_sgd,
                CAST(php_sgd_100 AS STRING) AS php_sgd_100, CAST(qar_sgd_100 AS STRING) AS qar_sgd_100,
                CAST(sar_sgd_100 AS STRING) AS sar_sgd_100, CAST(chf_sgd AS STRING) AS chf_sgd,
                CAST(thb_sgd_100 AS STRING) AS thb_sgd_100, CAST(aed_sgd_100 AS STRING) AS aed_sgd_100,
                CAST(vnd_sgd_100 AS STRING) AS vnd_sgd_100, 
                CAST(preliminary AS STRING) AS ex_rate_preliminary,
                CAST(timestamp AS STRING) AS ex_rate_timestamp
                from `{PROJECT_ID}.{STAGING_DATASET}.init_exchange_rates` as temp
        ''',
        dag = dag,
    )

    # Cast data types and remove duplicates interest rate
    distinct_interest = BigQueryOperator(
        task_id = 'distinct_interest_task',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.distinct_interest_rate` 
                AS SELECT DISTINCT PARSE_TIMESTAMP('%Y-%m-%d', end_of_day) AS Date, 
                CONCAT(temp.end_of_day, '-INR') AS INR_ID, *
                EXCEPT(
                    end_of_day, preliminary, timestamp,
                    interbank_overnight, interbank_1w, interbank_1m, interbank_2m, interbank_3m,
                    interbank_6m, interbank_12m, commercial_bills_3m, usd_sibor_3m, sgs_repo_overnight_rate,
                    on_rmb_facility_rate
                ),
                preliminary AS int_rate_preliminary, timestamp AS int_rate_timestamp,
                CAST(on_rmb_facility_rate AS STRING) AS on_rmb_facility_rate,
                FROM `{PROJECT_ID}.{STAGING_DATASET}.init_interest_rates` AS temp
        ''',
        dag = dag
    )

    # Remove Duplicates
    distinct_stock_prices = BigQueryOperator(
        task_id = 'distinct_stock_prices_task',
        use_legacy_sql = False,
        sql = f'''
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.distinct_hist_stock_prices` 
        AS SELECT DISTINCT *
        FROM `{PROJECT_ID}.{STAGING_DATASET}.init_hist_stock_prices`
        ''',
        dag = dag
    )

    # Remove Duplicates gold - ADDED
    distinct_gold = BigQueryOperator(
        task_id = 'distinct_gold_task',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.distinct_gold`
                AS SELECT DISTINCT CONCAT( FORMAT_TIMESTAMP('%Y-%m-%d', temp.Date) , '-GOLD-COMM') AS COMM_ID, *
                FROM `{PROJECT_ID}.{STAGING_DATASET}.init_gold` AS temp
        ''',
        dag = dag
    )

    # Remove Duplicates silver - ADDED
    distinct_silver = BigQueryOperator(
        task_id = 'distinct_silver_task',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.distinct_silver`
                AS SELECT DISTINCT CONCAT( FORMAT_TIMESTAMP('%Y-%m-%d', temp.Date) , '-SILVER-COMM') AS COMM_ID, *
                FROM `{PROJECT_ID}.{STAGING_DATASET}.init_silver` AS temp
        ''',
        dag = dag
    )

    # Remove Duplicates crude oil - ADDED
    distinct_crude_oil = BigQueryOperator(
        task_id = 'distinct_crude_oil_task',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.distinct_crude_oil`
                AS SELECT DISTINCT CONCAT( FORMAT_TIMESTAMP('%Y-%m-%d', temp.Date) , '-CRUDE OIL-COMM') AS COMM_ID, *
                FROM `{PROJECT_ID}.{STAGING_DATASET}.init_crude_oil` AS temp
        ''',
        dag = dag
    )

    # Add SMA to df
    sma_stock = PythonOperator(
        task_id = 'sma_stock_task',
        python_callable = sma_prices,
        dag = dag
    )

    # Add lag dates to df
    lag_int = PythonOperator(
        task_id = 'lag_int_task',
        python_callable = lag_int,
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

    # Load lag interest date data from GCS to BQ
    load_lag_interest = GoogleCloudStorageToBigQueryOperator(
        task_id = 'stage_lag_int_task',
        bucket = 'stock_prediction_is3107',
        source_objects = ['lag_interest.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.final_interest_rate',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    cast_int_rate = BigQueryOperator(
        task_id = 'cast_int_rate_task',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.casted_interest_rate` 
                AS SELECT DISTINCT Actual_Date, INR_ID,
                CAST(aggregate_volume AS FLOAT64) AS aggregate_volume,
                CAST(calculation_method AS STRING) AS calculation_method,
                CAST(comp_sora_1m AS FLOAT64) AS comp_sora_1m, CAST(comp_sora_3m AS FLOAT64) AS comp_sora_3m,
                CAST(comp_sora_6m AS FLOAT64) AS comp_sora_6m,
                CAST(highest_transaction AS FLOAT64) AS highest_transaction,
                CAST(lowest_transaction AS FLOAT64) AS lowest_transaction,
                CAST(on_rmb_facility_rate AS STRING) AS on_rmb_facility_rate,
                CAST(published_date AS STRING) AS published_date,
                CAST(sor_average AS FLOAT64) AS sor_average,
                CAST(sora AS FLOAT64) AS sora, CAST(sora_index AS FLOAT64) AS sora_index,
                CAST(standing_facility_borrow AS STRING) AS standing_facility_borrow,
                CAST(standing_facility_deposit AS STRING) AS standing_facility_deposit,
                CAST(int_rate_preliminary AS INTEGER) AS int_rate_preliminary, 
                CAST(int_rate_timestamp AS STRING) AS int_rate_timestamp,
                `Date`
                FROM `{PROJECT_ID}.{STAGING_DATASET}.final_interest_rate` AS temp
        '''
    )

    # Combine gold, silver and crude oil dataframes into a commodities dataframe
    combine_commodities = PythonOperator(
        task_id = 'commodities_task',
        python_callable = commodities,
        dag = dag
    )

    # Load commodities data from GCS to BQ
    load_commodities = GoogleCloudStorageToBigQueryOperator(
        task_id = 'stage_commodities_task',
        bucket = 'stock_prediction_is3107',
        source_objects = ['final_commodities.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.final_commodity_prices',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )    

    ############################
    # Define Tasks Hierarchy   #
    ############################
    [distinct_exchange, distinct_interest, distinct_stock_prices, distinct_gold, distinct_silver, distinct_crude_oil] 
    distinct_stock_prices >> sma_stock >> load_sma
    distinct_interest >> lag_int >> load_lag_interest >> cast_int_rate
    [distinct_exchange, load_lag_interest, load_sma] 
    [distinct_gold, distinct_silver, distinct_crude_oil] >> combine_commodities >> load_commodities

    return transform_taskgroup
