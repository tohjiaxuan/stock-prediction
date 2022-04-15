from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage

import pandas as pd
import os

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'

def build_gcs_taskgroup(dag: DAG) -> TaskGroup:
    gcs_taskgroup = TaskGroup(group_id = 'gcs_taskgroup')

    # Push stock data from XCOM to Cloud
    def push_stock_price(**kwargs):
        scraping_data = kwargs['ti'].xcom_pull(task_ids='stock_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_stock_scraping_data')
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/stock_prices.parquet', engine='pyarrow', index=False)
            print("Pushing Historical Stock Prices to Cloud")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/stock_prices.parquet', engine='pyarrow', index=False)
            print("Pushing New Stock Prices to Cloud")

    # Push e/r data from XCOM to Clouds
    def push_exchange_rate(**kwargs):
        scraping_data = kwargs['ti'].xcom_pull(task_ids='exchange_rate_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_exchange_rate_scraping_data')
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/exchange_rate.parquet', engine='pyarrow', index=False)
            print("Pushing Initialisation Exchange Rates to Cloud")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/exchange_rate.parquet', engine='pyarrow', index=False)
            print("Pushing New Exchange Rates to Cloud")
    
    # Push i/r data from XCOM to Clouds
    def push_interest_rate(**kwargs):
        scraping_data = kwargs['ti'].xcom_pull(task_ids='interest_rate_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_interest_rate_scraping_data')
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/interest_rate.parquet', engine='pyarrow', index=False)
            print("Pushing Initialisation Interest Rates to Cloud")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/interest_rate.parquet', engine='pyarrow', index=False)
            print("Pushing New Interest Rates to Cloud")

    # Push gold data from XCOM to Clouds
    def push_gold(**kwargs):
        scraping_data = kwargs['ti'].xcom_pull(task_ids='gold_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_gold_scraping_data')
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/gold.parquet', engine='pyarrow', index=False)
            print('Pushing Initialisation Gold Prices to Cloud')
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/gold.parquet', engine='pyarrow', index=False)
            print('Pushing New Gold Prices to Cloud')

    # Push silver data from XCOM to Clouds
    def push_silver(**kwargs):
        scraping_data = kwargs['ti'].xcom_pull(task_ids='silver_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_silver_scraping_data')
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/silver.parquet', engine='pyarrow', index=False)
            print('Pushing Initialisation Silver Prices to Cloud')
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/silver.parquet', engine='pyarrow', index=False)
            print('Pushing New Silver Prices to Cloud')

    # Push crude oil data from XCOM to Clouds
    def push_crude_oil(**kwargs):
        scraping_data = kwargs['ti'].xcom_pull(task_ids='crude_oil_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_crude_oil_scraping_data')
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/crude_oil.parquet', engine='pyarrow', index=False)
            print('Pushing Initialisation Crude Oil Prices to Cloud')
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/crude_oil.parquet', engine='pyarrow', index=False)
            print('Pushing New Crude Oil Prices to Cloud')
    
    ####################
    # Pushing to Cloud #
    ####################

    # Push Historical Stock Prices to Cloud
    stock_cloud = PythonOperator(
        task_id = 'push_stock_cloud_data',
        trigger_rule = 'none_failed',
        python_callable = push_stock_price,
        provide_context = True,
        dag = dag
    )

    # Push Exchange Rates to Cloud
    exchange_cloud = PythonOperator(
        task_id = 'push_exchange_cloud_data',
        trigger_rule = 'none_failed',
        python_callable = push_exchange_rate,
        provide_context = True,
        dag = dag
    )

    # Push Interest Rates to Cloud
    interest_cloud = PythonOperator(
        task_id = 'push_interest_cloud_data',
        trigger_rule = 'none_failed',
        python_callable = push_interest_rate,
        provide_context = True,
        dag = dag
    )

    # Push Gold to Cloud
    gold_cloud = PythonOperator(
        task_id = 'push_gold_cloud_data',
        trigger_rule = 'none_failed',
        python_callable = push_gold,
        provide_context = True,
        dag = dag
    )

    # Push Silver to Cloud
    silver_cloud = PythonOperator(
        task_id = 'push_silver_cloud_data',
        trigger_rule = 'none_failed',
        python_callable = push_silver,
        provide_context = True,
        dag = dag
    )

    # Push Crude Oil to Cloud
    crude_oil_cloud = PythonOperator(
        task_id = 'push_crude_oil_cloud_data',
        trigger_rule = 'none_failed',
        python_callable = push_crude_oil,
        provide_context = True,
        dag = dag
    )

    [stock_cloud, exchange_cloud, interest_cloud, gold_cloud, silver_cloud, crude_oil_cloud]

    return gcs_taskgroup