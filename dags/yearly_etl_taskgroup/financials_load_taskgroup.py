from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
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


def build_financials_load_taskgroup(dag: DAG) -> TaskGroup:
    financials_load_taskgroup = TaskGroup(group_id = 'financials_load_tg')

    # Load data into D_FINANCIALS Dimension table
    d_financials_table = BigQueryExecuteQueryOperator(
        task_id = 'd_financials_table',
        use_legacy_sql = False,
        params = {
            'project_id': PROJECT_ID,
            'staging_dataset': STAGING_DATASET,
            'dwh_dataset': DWH_DATASET
        },
        destination_dataset_table=f'{PROJECT_ID}:{DWH_DATASET}.D_FINANCIALS',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        sql = './sql/D_financials.sql',
        dag = dag
    )

    # Load data into D_INFLATION Dimension table
    d_inflation_table = BigQueryExecuteQueryOperator(
        task_id = 'd_inflation_table',
        use_legacy_sql = False,
        params = {
            'project_id': PROJECT_ID,
            'staging_dataset': STAGING_DATASET,
            'dwh_dataset': DWH_DATASET
        },
        destination_dataset_table=f'{PROJECT_ID}:{DWH_DATASET}.D_INFLATION',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        sql = './sql/D_inflation.sql',
        dag = dag
    )

    # Kickstarts Loading
    start_loading = DummyOperator(
        task_id = 'start_loading', 
        dag = dag
    )
    
    # TASK DEPENDENCIES

    start_loading >> [d_financials_table, d_inflation_table]
    
    return financials_load_taskgroup