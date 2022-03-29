from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.subdag import SubDagOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage

from airflow.utils.task_group import TaskGroup
from extract_taskgroup import build_extract_taskgroup
from gcs_taskgroup import build_gcs_taskgroup
from stage_taskgroup import build_stage_taskgroup
from transform_taskgroup import build_transform_taskgroup
from load_taskgroup import build_load_taskgroup

import json
import os
import numpy as np
import pandas as pd
import requests

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'

curr_date = datetime.today().strftime('%Y-%m-%d')

# Add retry_delay later on when pipeline is up
default_args = {
    'owner': 'Nicole',
    'depends_on_past': False,
    'email': ['nicole@png.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # Remember to change this when the actual project kickstarts
    'start_date': datetime(2022, 3, 15)
}

with DAG(
    dag_id="daily_financial_news",
    schedule_interval="@daily",
    description = 'DAG for creation of data warehouse for financial news (Daily)',
    default_args=default_args,
    catchup = False
) as dag:
    with TaskGroup("extract", prefix_group_id = False) as section_1:
        extract_taskgroup = build_extract_taskgroup(dag=dag)
    with TaskGroup("gcs", prefix_group_id = False) as section_2:
        gcs_taskgroup = build_gcs_taskgroup(dag=dag)
    with TaskGroup("stage", prefix_group_id = False) as section_3:
        stage_taskgroup = build_stage_taskgroup(dag=dag)
    with TaskGroup("transform", prefix_group_id = False) as section_4:
        transform_taskgroup = build_transform_taskgroup(dag=dag)
    with TaskGroup("load", prefix_group_id = False) as section_5:
        load_taskgroup = build_load_taskgroup(dag=dag)

    section_1 >> section_2 >> section_3 >> section_4 >> section_5