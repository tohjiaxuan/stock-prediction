from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

import cchardet
import json
import os
import pandas as pd
import requests
import urllib.request



headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'


def build_financials_stage_taskgroup(dag: DAG) -> TaskGroup:
    financials_stage_taskgroup = TaskGroup(group_id = 'financials_stage_tg')

    ##############
    # Staging    #
    ##############


    # Load net income from GCS to BigQuery (BQ)
    stage_netincome = GCSToBigQueryOperator(
        task_id = 'stage_netincome',
        bucket = 'stock_prediction_is3107',
        source_objects = ['netincome.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.netincome',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    # Load assets from GCS to BQ
    stage_assets = GCSToBigQueryOperator(
        task_id = 'stage_assets',
        bucket = 'stock_prediction_is3107',
        source_objects = ['assets.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.assets',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    # Load liab from GCS to BQ
    stage_liab = GCSToBigQueryOperator(
        task_id = 'stage_liab',
        bucket = 'stock_prediction_is3107',
        source_objects = ['liab.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.liab',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    # Load equity from GCS to BQ
    stage_equity = GCSToBigQueryOperator(
        task_id = 'stage_equity',
        bucket = 'stock_prediction_is3107',
        source_objects = ['equity.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.equity',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    # Load div from GCS to BQ
    stage_div = GCSToBigQueryOperator(
        task_id = 'stage_div',
        bucket = 'stock_prediction_is3107',
        source_objects = ['div.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.div',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    # Load inflation from GCS to BQ
    stage_inflation = GCSToBigQueryOperator(
        task_id = 'stage_inflation',
        bucket = 'stock_prediction_is3107',
        source_objects = ['inflation.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.inflation',
        write_disposition='WRITE_TRUNCATE',
        autodetect = True,
        source_format = 'PARQUET',
        dag = dag
    )

    # kickstart staging
    start_staging = DummyOperator(
        task_id = 'start_staging',
        dag = dag
    )

    # end staging
    staging_complete = DummyOperator(
        task_id = 'staging_complete',
        dag = dag
    )
    
    # TASK DEPENDENCIES

    start_staging >> [stage_netincome, stage_assets, stage_liab, stage_equity, stage_div, stage_inflation] >> staging_complete

    return financials_stage_taskgroup