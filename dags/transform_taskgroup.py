from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage
from airflow.utils.task_group import TaskGroup

import json
import os
import pandas as pd
import pandas_ta as ta
import requests
import urllib.request
import yfinance as yf 

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'

def build_transform_taskgroup(dag: DAG) -> TaskGroup:
    transform_taskgroup = TaskGroup(group_id = 'transform_taskgroup')

    ############################
    # Define Python Functions  #
    ############################

    # Todo: need to edit to take in the normal one... daily after dwh is up
    def sma_prices(**kwargs):
        bq_client = bigquery.Client()
        query = "select * from `stockprediction-344203.stock_prediction_staging_dataset.distinct_hist_stock_prices`"
        df = bq_client.query(query).to_dataframe()

        # Check for null values, if present, do ffill
        if (df.isnull().values.any() == True):
            df = df.fillna(method='ffill')

        # Calculate GC
        df["GC"] = df.ta.sma(50, append=True) > df.ta.sma(200, append=True)

        # Calculate DC
        df["DC"] = df.ta.sma(50, append=True) < df.ta.sma(200, append=True)

        print("Transforming Stocks Data Complete")
        df.to_parquet('gs://stock_prediction_is3107/final_stock.parquet')
        return df
        
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
            end_of_day, __index_level_0__, preliminary, timestamp,
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
        except(
            __index_level_0__
        )
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