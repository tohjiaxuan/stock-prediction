from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.cloud import storage
import cchardet
import json
import numpy as np
import os
import pandas as pd
import requests
import urllib.request

from financials_extract_taskgroup import build_financials_extract_taskgroup
from financials_gcs_taskgroup import build_financials_gcs_taskgroup
from financials_transform_taskgroup import build_financials_transform_taskgroup
from financials_load_taskgroup import build_financials_load_taskgroup
from financials_schema_taskgroup import build_financials_schema_taskgroup
from financials_postgres_taskgroup import build_financials_postgres_taskgroup
from financials_stage_taskgroup import build_financials_stage_taskgroup



headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'

default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email': ['vickiyew@gmail.com'], # add your own email here
     'email_on_failure': True,
     'email_on_retry': True,
     'retries': 2,
     'retry_delay': timedelta(minutes=10),
     'start_date': datetime(2022, 3, 15)
    }

def get_latest_date():
    """ Retrieves the latest date of the data in the data warehouse

        Returns
        -------
        date
         
    """
    bq_client = bigquery.Client()
    query = "select MAX(`year`) from `stockprediction-344203.stock_prediction_datawarehouse.D_INFLATION`"
    df = bq_client.query(query).to_dataframe()
    recent_date = df['f0_'].values[0]
    return recent_date

def days_difference():
    """ Calculates difference in the number of days between latest date of data in the data warehouse and today's date

        Returns
        -------
        float
         
    """
    today = np.datetime64('today')
    difference = (today - get_latest_date()).astype('timedelta64[D]') / np.timedelta64(1, 'D')
    return difference

def d_inflation_empty():
    """ Checks if D_INFLATION table is empty

        Returns
        -------
        boolean
            TRUE - empty
            FALSE - not empty
         
    """
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = 'select COUNT(*) from `stockprediction-344203.stock_prediction_datawarehouse.D_INFLATION`'
    df = bq_client.query(query).to_dataframe()
    df_length = df['f0_'].values[0]
    if (df_length != 0):
        return False
    else:
        return True

def decide_path():
    """ Tells DAG to either skip all downstream tasks and straight away trigger the Daily DAG OR
        carry out all downstream tasks (ETL) and trigger the Daily DAG

        Returns
        -------
        taskid 
            Tells the DAG which path to take
         
    """
    # if empty, start to extract task
    if d_inflation_empty():
        return 'start_extract_task'
    else:
        # only run the DAG if the data is not up-to-date.
        if days_difference() > 365:
            return 'start_extract_task'
        # else, skip till the end of DAG.
        else: 
            return 'end_yearly'


with DAG(
    dag_id="yearly_dag_etl",
    schedule_interval="@daily", # set to daily so that the Daily DAG can be triggered everyday
    description = 'DAG for creation of data warehouse (Yearly)',
    default_args=default_args,
    catchup = False
) as dag:
    
    # kickstart DAG
    start_yearly = BashOperator(
        task_id = 'start_yearly',
        bash_command = 'echo start',
        dag = dag
    )

    # completed downstream activities
    end_yearly = BashOperator(
        task_id = 'end_yearly',
        bash_command = 'echo end',
        trigger_rule = 'all_done',
        dag = dag
    )

    # BranchPythonOperator tells DAG which path to take
    dag_path = BranchPythonOperator(
        task_id = 'dag_path',
        python_callable = decide_path,
        do_xcom_push = False,
        provide_context = True,
        dag = dag
    )

    # Instruct DAG to begin downstream tasks (ETL)
    start_extract_task = BashOperator(
        task_id = 'start_extract_task',
        bash_command = 'echo start',
        dag = dag
    )

    # Trigger Daily DAG
    trigger = TriggerDagRunOperator (
        task_id='trigger_task',
        trigger_dag_id="daily_dag",
        dag=dag)

    
    # importing the various taskgroups that make up the Yearly DAG
    with TaskGroup("postgres", prefix_group_id=False) as section_postgres:
        financials_postgres_taskgroup = build_financials_postgres_taskgroup(dag=dag)
    with TaskGroup("schema", prefix_group_id=False) as section_0:
        financials_schema_taskgroup = build_financials_schema_taskgroup(dag=dag)
    with TaskGroup("extract", prefix_group_id=False) as section_1:
        financials_extract_taskgroup = build_financials_extract_taskgroup(dag=dag)
    with TaskGroup("gcs", prefix_group_id=False) as section_2:
        financials_gcs_taskgroup = build_financials_gcs_taskgroup(dag=dag)
    with TaskGroup("stage", prefix_group_id=False) as section_3:
        financials_stage_taskgroup = build_financials_stage_taskgroup(dag=dag)
    with TaskGroup("transform", prefix_group_id=False) as section_4:
        financials_transform_taskgroup = build_financials_transform_taskgroup(dag=dag)
    with TaskGroup("load", prefix_group_id=False) as section_5:
        financials_load_taskgroup = build_financials_load_taskgroup(dag=dag)
    
    # TASK DEPENDENCIES
    
    start_yearly >> section_0 >> dag_path >> [start_extract_task, end_yearly]
    start_extract_task >> section_1 >> section_2 >> section_3 >> section_4 >> section_postgres >> section_5 >> end_yearly >> trigger