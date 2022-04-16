from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import BranchPythonOperator
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
    """Creates a taskgroup to load data into the BigQuery data warehouse. 
    Parameters
    ----------
    dag: An airflow DAG

    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    financials_load_taskgroup = TaskGroup(group_id = 'financials_load_tg')

    def check_to_load_or_skip(ti):
        """Checks whether to load data into the data warehouse, or to skip loading. 
        Parameters
        ----------
        ti

        Returns
        -------
        task id
           Tells dag which part to take - load data or skip loading
        """
        instruction = ti.xcom_pull(task_ids='dag_path')
        if instruction == 'start_extract_task':
            return 'd_financials_table'
        else:
            return 'do_not_load'

    # BranchPythonOperator tells DAG which path to take
    load_path = BranchPythonOperator(
        task_id = 'load_path',
        python_callable = check_to_load_or_skip,
        do_xcom_push = False,
        provide_context = True,
        dag = dag
    )

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
        trigger_rule = 'one_success',
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
        trigger_rule = 'one_success',
        dag = dag
    )
    
    # signal to not load
    do_not_load = BashOperator(
        task_id="do_not_load",
        bash_command="echo do_not_load",
        dag=dag
    )

    # signal the end of task group
    end = BashOperator(
        task_id="end",
        bash_command="echo end",
        trigger_rule="all_done",
        dag=dag
    )

    # TASK DEPENDENCIES

    load_path >> [d_financials_table, do_not_load]
    d_financials_table >> d_inflation_table >> end
    do_not_load >> end
    
    return financials_load_taskgroup