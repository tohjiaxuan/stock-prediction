from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from airflow.utils.task_group import TaskGroup

import json
import os
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bq_client = bigquery.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'


def build_transform_taskgroup(dag: DAG) -> TaskGroup:
    transform_taskgroup = TaskGroup(group_id = 'transform_taskgroup')

    ############################
    # Define Python Functions  #
    ############################

    join_financial_news = BigQueryOperator(
    task_id = 'join_financial_news',
    use_legacy_sql = False,
    sql = f'''
            create or replace table `{PROJECT_ID}.{STAGING_DATASET}.join_financial_news` as 
            select distinct * from 
            (select cast(ticker as string) as Ticker, cast(title as string) as Title, cast(date as string) as Date, cast(link as string) as Link, cast(source as string) as Source, cast(comments as string) as Comments
                from `{PROJECT_ID}.{STAGING_DATASET}.sginvestor_news` union distinct
            select cast(ticker as string) as Ticker, cast(title as string) as Title, cast(date as string) as Date, cast(link as string) as Link, cast(source as string) as Source, cast(comments as string) as Comments
                from `{PROJECT_ID}.{STAGING_DATASET}.sginvestor_blog_news` union distinct
            select cast(ticker as string) as Ticker, cast(title as string) as Title, cast(date as string) as Date, cast(link as string) as Link, cast(source as string) as Source, cast(comments as string) as Comments
                from `{PROJECT_ID}.{STAGING_DATASET}.yahoofinance_news` 
            ) temp
    ''',
    dag = dag
    )

    start_transformation = DummyOperator(
        task_id = 'start_transformation',
        dag = dag
    )

    end_transformation = BashOperator(
        task_id="end_transformation",
        bash_command="echo end_transformation",
        trigger_rule="all_done",
        dag=dag
    )


    ############################
    # Define Airflow Operators #
    ############################

    # # Remove Duplicates
    # distinct_stock_prices = BigQueryOperator(
    #     task_id = 'distinct_stock_prices_task',
    #     use_legacy_sql = False,
    #     sql = f'''
    #     create or replace table `{PROJECT_ID}.{STAGING_DATASET}.distinct_hist_stock_prices` 
    #     as select distinct *
    #     from `{PROJECT_ID}.{STAGING_DATASET}.init_hist_stock_prices`
    #     ''',
    #     dag = dag
    # )


    ############################
    # Define Tasks Hierarchy   #
    ############################
    start_transformation >> join_financial_news >> end_transformation

    return transform_taskgroup