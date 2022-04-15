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


def build_financials_load_taskgroup(dag: DAG) -> TaskGroup:
    financials_load_taskgroup = TaskGroup(group_id = 'financials_load_tg')

    ## INTO DATAWAREHOUSE
    ###########
    # Loading #
    # (INIT)  #
    ###########

    '''
    create_stocks_data = BigQueryOperator(
        task_id = 'create_stocks_data',
        use_legacy_sql = False,
        params = {
            'project_id': PROJECT_ID,
            'staging_dataset': STAGING_DATASET,
            'dwh_dataset': DWH_DATASET
        },
        sql = './sql/F_stock.sql',
        dag = dag
    )
    '''

    create_d_financials = BigQueryOperator(
        task_id = 'create_d_financials',
        use_legacy_sql = False,
        params = {
            'project_id': PROJECT_ID,
            'staging_dataset': STAGING_DATASET,
            'dwh_dataset': DWH_DATASET
        },
        sql = './sql/D_financials.sql',
        dag = dag
    )

    create_d_inflation = BigQueryOperator(
        task_id = 'create_d_inflation',
        use_legacy_sql = False,
        params = {
            'project_id': PROJECT_ID,
            'staging_dataset': STAGING_DATASET,
            'dwh_dataset': DWH_DATASET
        },
        sql = './sql/D_inflation.sql',
        dag = dag
    )


    ###########
    # Loading #
    # YEARLY  #
    ###########

    # not sure, to figure out.
    '''
    append_F_stock = DummyOperator(
        task_id = 'append_F_stock',
        dag = dag
    )
    '''

    append_D_financials = BigQueryOperator(
        task_id = 'append_D_financials',
        use_legacy_sql = False,
        sql = f'''
            INSERT `{PROJECT_ID}.{DWH_DATASET}.D_financials` 
            SELECT DISTINCT * FROM `{PROJECT_ID}.{STAGING_DATASET}.reformat_financials_ratios_yearly`
        ''',
        dag = dag
    )

    append_D_inflation = BigQueryOperator(
        task_id = 'append_D_inflation',
        use_legacy_sql = False,
        sql = f'''
            INSERT `{PROJECT_ID}.{DWH_DATASET}.D_inflation` 
            SELECT DISTINCT * FROM `{PROJECT_ID}.{STAGING_DATASET}.inflation_key`
        ''',
        dag = dag
    )

    start_loading = DummyOperator(
        task_id = 'start_loading',
        dag = dag
    )

    def check_financials_choose_load(**kwargs):
        client_bq = bigquery.Client(project=PROJECT_ID)
        table_ref = "stockprediction-344203.stock_prediction_datawarehouse.D_financials"
        try:
            table = client_bq.get_table(table_ref)
            if table:
                return 'append_D_financials'
            
        except NotFound as error:
            return 'create_d_financials'

    def check_inflation_choose_load(**kwargs):
        client_bq = bigquery.Client(project=PROJECT_ID)
        table_ref = "stockprediction-344203.stock_prediction_datawarehouse.D_inflation"
        try:
            table = client_bq.get_table(table_ref)
            if table:
                return 'append_D_inflation'
            
        except NotFound as error:
            return 'create_d_inflation'

    check_financials_choose_load_path = BranchPythonOperator(
        task_id = 'check_financials_choose_load_path',
        python_callable = check_financials_choose_load,
        do_xcom_push = False,
        dag = dag
    )

    check_inflation_choose_load_path = BranchPythonOperator(
        task_id = 'check_inflation_choose_load_path',
        python_callable = check_inflation_choose_load,
        do_xcom_push = False,
        dag = dag
    )

    end_loading_init = DummyOperator(
        task_id = 'end_loading_init',
        dag = dag
    )

    end_loading_yearly = DummyOperator(
        task_id = 'end_loading_yearly',
        dag = dag
    )

    start_loading >> [check_financials_choose_load_path, check_inflation_choose_load_path]
    check_financials_choose_load_path >> [create_d_financials, append_D_financials]
    check_inflation_choose_load_path >> [create_d_inflation, append_D_inflation]

    [create_d_financials, create_d_inflation] >> end_loading_init
    [append_D_financials, append_D_inflation] >> end_loading_yearly

    return financials_load_taskgroup