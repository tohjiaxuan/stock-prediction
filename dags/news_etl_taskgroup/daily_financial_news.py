from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage

from airflow.utils.task_group import TaskGroup
from extract_news_taskgroup import build_extract_news_taskgroup
from gcs_news_taskgroup import build_gcs_news_taskgroup
from stage_news_taskgroup import build_stage_news_taskgroup
from transform_news_taskgroup import build_transform_news_taskgroup
from load_news_taskgroup import build_load_news_taskgroup
from postgres_news_taskgroup import build_postgres_news_taskgroup

import json
import logging
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)

# Define arguments for the DAG
default_args = {
    'owner': 'Nicole',
    'depends_on_past': False,
    'email': [''], # Can change to the user of choice
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2, 
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 3, 15) # Change this date when first initialised
}

def check_news(**kwargs):
    """Checks if there is any new stock data available

    Parameters
    ----------
    **kwargs: pass any keyword argument

    Returns
    -------
    taskid
        Tells the DAG which path to take
    """
    sginvestor = kwargs['task_instance'].xcom_pull(task_ids='sginvestor_scraping')
    sginvestor_blog = kwargs['task_instance'].xcom_pull(task_ids='sginvestor_blog_scraping')
    yahoofinance = kwargs['task_instance'].xcom_pull(task_ids='yahoofinance_scraping')
    if sginvestor.empty and sginvestor_blog.empty and yahoofinance.empty:
        logging.info('No new news data detected, end DAG')
        return 'end_task'
    logging.info('Starting to push data to GCS')
    return 'start_gcs_task'

with DAG(
    dag_id="daily_financial_news",
    schedule_interval="@daily",
    description = 'DAG for creation of data warehouse for financial news (Daily)',
    default_args=default_args,
    catchup = False
) as dag:

    # Signifies the start of the daily DAG
    start_daily = BashOperator(
        task_id = 'start_task',
        bash_command = 'echo start',
        dag = dag
    )

    # Signifies the end of daily DAG
    end_daily = BashOperator(
        task_id = 'end_task',
        bash_command = 'echo end',
        trigger_rule = 'none_failed',
        dag = dag
    )

    # Chooses the path for the DAG to run
    dag_path = BranchPythonOperator(
        task_id = 'dag_path_task',
        python_callable = check_news,
        do_xcom_push = True,
        provide_context = True,
        dag = dag
    )

    with TaskGroup("extract", prefix_group_id = False) as section_1:
        extract_news_taskgroup = build_extract_news_taskgroup(dag=dag)
    with TaskGroup("gcs", prefix_group_id = False) as section_2:
        gcs_news_taskgroup = build_gcs_news_taskgroup(dag=dag)
    with TaskGroup("stage", prefix_group_id = False) as section_3:
        stage_news_taskgroup = build_stage_news_taskgroup(dag=dag)
    with TaskGroup("transform", prefix_group_id = False) as section_4:
        transform_news_taskgroup = build_transform_news_taskgroup(dag=dag)
    with TaskGroup("load", prefix_group_id = False) as section_5:
        load_news_taskgroup = build_load_news_taskgroup(dag=dag)
    with TaskGroup("postgres", prefix_group_id = False) as postgres:
        postgres_news_taskgroup = build_postgres_news_taskgroup(dag=dag)

    # Echos the start of interactions with cloud
    start_gcs = BashOperator(
        task_id = 'start_gcs_task',
        bash_command = 'echo start',
        dag = dag
    )

    # Task hierarchy 
    start_daily >> section_1 >> dag_path >> [start_gcs, end_daily]
    start_gcs >> section_2 >> section_3 >> section_4 >> postgres >> section_5 >> end_daily

