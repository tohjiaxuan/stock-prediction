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

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'


def build_financials_gcs_taskgroup(dag: DAG) -> TaskGroup:
    financials_gcs_taskgroup = TaskGroup(group_id = 'financials_gcs_tg')

    ############################
    # Push to GCS From XCOMS #
    ############################


    # Pull data from XCOMs for financials and push to cloud

    def push_netincome(ti):
        netincome = ti.xcom_pull(task_ids='income_scraping')
        netincome.to_parquet('gs://stock_prediction_is3107/netincome.parquet')

    def push_assets(ti):
        assets = ti.xcom_pull(task_ids='assets_scraping')
        assets.to_parquet('gs://stock_prediction_is3107/assets.parquet')
    
    def push_liab(ti):
        liab = ti.xcom_pull(task_ids='liab_scraping')
        liab.to_parquet('gs://stock_prediction_is3107/liab.parquet')
    
    def push_equity(ti):
        eq = ti.xcom_pull(task_ids='equity_scraping')
        eq.to_parquet('gs://stock_prediction_is3107/equity.parquet')
    
    def push_dividends(ti):
        div = ti.xcom_pull(task_ids='dividends_scraping')
        div.to_parquet('gs://stock_prediction_is3107/div.parquet')

    def push_inflation(ti):
        inflation = ti.xcom_pull(task_ids='inflation_scraping')
        inflation.to_parquet('gs://stock_prediction_is3107/inflation.parquet')


    # Operator to push to cloud
    netincome_cloud = PythonOperator(
        task_id = 'netincome_cloud',
        python_callable = push_netincome,
        dag = dag
    )

    assets_cloud = PythonOperator(
        task_id = 'assets_cloud',
        python_callable = push_assets,
        dag = dag
    )

    liab_cloud = PythonOperator(
        task_id = 'liab_cloud',
        python_callable = push_liab,
        dag = dag
    )

    equity_cloud = PythonOperator(
        task_id = 'equity_cloud',
        python_callable = push_equity,
        dag = dag
    )

    dividends_cloud = PythonOperator(
        task_id = 'dividends_cloud',
        python_callable = push_dividends,
        dag = dag
    )

    inflation_cloud = PythonOperator(
        task_id = 'inflation_cloud',
        python_callable = push_inflation,
        dag = dag
    )

    start_gcs = DummyOperator(
        task_id = 'start_gcs',
        dag = dag
    )

    loaded_gcs = DummyOperator(
        task_id = 'loaded_gcs',
        dag = dag
    )

    start_gcs >> [netincome_cloud, assets_cloud, liab_cloud, equity_cloud, dividends_cloud, inflation_cloud] >> loaded_gcs

    return financials_gcs_taskgroup