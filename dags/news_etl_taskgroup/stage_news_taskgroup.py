from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import DAG
from google.cloud import bigquery
from google.cloud import storage

import os

# Establish connection with staging dataset in BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'

def build_stage_news_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup for loading of data from GCS into staging tables

    Parameters
    ----------
    dag: An airflow DAG

    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    stage_news_taskgroup = TaskGroup(group_id = "stage_news_taskgroup")

    # Load Yahoo Finance data from GCS to BQ
    stage_yahoofinance = GCSToBigQueryOperator(
        task_id = 'stage_yahoofinance',
        bucket = 'stock_prediction_is3107',
        source_objects = ['yahoofinance_news.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.yahoofinance_news',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    # Load SG Investor data from GCS to BQ
    stage_sginvestor = GCSToBigQueryOperator(
        task_id = 'stage_sginvestor',
        bucket = 'stock_prediction_is3107',
        source_objects = ['sginvestor_news.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.sginvestor_news',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    # Load SG Investor Blog data from GCS to BQ
    stage_sginvestor_blog = GCSToBigQueryOperator(
        task_id = 'stage_sginvestor_blog',
        bucket = 'stock_prediction_is3107',
        source_objects = ['sginvestor_blog_news.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.sginvestor_blog_news',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    [stage_yahoofinance, stage_sginvestor, stage_sginvestor_blog]
    return stage_news_taskgroup