from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, date
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
import cchardet
import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib.request
import os
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.task_group import TaskGroup
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
import financials_dwh_schemas as ss
from google.oauth2 import service_account
from google.cloud import exceptions
import argparse
import logging
logging.basicConfig(level=logging.INFO)

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'


def build_financials_schema_taskgroup(dag: DAG) -> TaskGroup:
    financials_schema_taskgroup = TaskGroup(group_id = 'financials_schema_tg')

    def create_dataset(**kwargs):
        '''
        Create dataset in a project
        Parameteres:
            - dataset_id (str): ID of dataset to be created
            - client (obj): client object
        '''
        client = bigquery.Client(project=PROJECT_ID)

        try:
            dataset = client.get_dataset(DWH_DATASET)
        except exceptions.NotFound:
            logging.info(f'Creating dataset {DWH_DATASET}')
            client.create_dataset(DWH_DATASET)
        else:
            logging.info(f'Dataset not created. {DWH_DATASET} already exists.')

    def create_table(**kwargs):
        client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = client.dataset(DWH_DATASET)
        for table_id in ss.tables:
            schema = eval('ss.'+table_id)
            table_ref = dataset_ref.table(table_id)
            table = bigquery.Table(table_ref, schema=schema)
            try:
                table = client.create_table(table)
            except exceptions.Conflict:
                logging.info(f'Table not created. {table_id} already exists')
            else:
                logging.info(f'Created table {DWH_DATASET}.{table_id}')

    create_tables = PythonOperator(
        task_id = 'create_tables',
        python_callable = create_table,
        dag = dag
    )

    create_tables
    return financials_schema_taskgroup