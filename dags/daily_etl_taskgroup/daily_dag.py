from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage

from airflow.utils.task_group import TaskGroup
from extract_taskgroup import build_extract_taskgroup
from gcs_taskgroup import build_gcs_taskgroup
from stage_taskgroup import build_stage_taskgroup
from transform_taskgroup import build_transform_taskgroup
from load_taskgroup import build_load_taskgroup
from postgres_processing_daily import build_daily_postgres_taskgroup

import json
import os
import numpy as np
import pandas as pd
import pandas_ta as ta
import requests
import urllib.request
import yfinance as yf 

yf.pdr_override()

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}

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
    'retries': 1, # Remember to change this when the actual project kickstarts
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 3, 15)
}

def check_stocks(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='stock_scraping_data')
    if df.empty:
        return 'end_task'
    return 'start_gcs_task'

with DAG(
    dag_id="daily_dag",
    schedule_interval="@daily",
    description = 'DAG for creation of data warehouse (Daily)',
    default_args=default_args,
    catchup = False
) as dag:
    
    start_daily = BashOperator(
        task_id = 'start_task',
        bash_command = 'echo start',
        dag = dag
    )

    end_daily = BashOperator(
        task_id = 'end_task',
        bash_command = 'echo end',
        trigger_rule = 'none_failed',
        dag = dag
    )

    dag_path = BranchPythonOperator(
        task_id = 'dag_path_task',
        python_callable = check_stocks,
        do_xcom_push = False,
        provide_context = True,
        dag = dag
    )

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
    with TaskGroup("dailypostgres", prefix_group_id = False) as section_postgres:
        daily_postgres_taskgroup = build_daily_postgres_taskgroup(dag=dag)
    start_gcs = BashOperator(
        task_id = 'start_gcs_task',
        bash_command = 'echo start',
        dag = dag
    )

    start_daily >> section_1 >> dag_path >> [start_gcs, end_daily]
    start_gcs >> section_2 >> section_3 >> section_4 >> section_postgres >> section_5 >> end_daily
