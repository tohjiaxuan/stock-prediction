from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import DAG
from google.cloud import bigquery
from google.cloud import storage

import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'

def build_stage_taskgroup(dag: DAG) -> TaskGroup:
    stage_taskgroup = TaskGroup(group_id = "stage_taskgroup")
    # Load stock prices data from GCS to BQ
    load_stock_prices = GoogleCloudStorageToBigQueryOperator(
        task_id = 'stage_stock_prices',
        bucket = 'stock_prediction_is3107',
        source_objects = ['stock_prices.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.init_hist_stock_prices',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    # Load interest rate data from GCS to BQ
    load_interest_rates = GoogleCloudStorageToBigQueryOperator(
        task_id = 'stage_interest_rate',
        bucket = 'stock_prediction_is3107',
        source_objects = ['interest_rate.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.init_interest_rates',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    # Load exchange rate data from GCS to BQ
    load_exchange_rates = GoogleCloudStorageToBigQueryOperator(
        task_id = 'stage_exchange_rate',
        bucket = 'stock_prediction_is3107',
        source_objects = ['exchange_rate.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.init_exchange_rates',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    [load_stock_prices, load_interest_rates, load_exchange_rates]

    return stage_taskgroup