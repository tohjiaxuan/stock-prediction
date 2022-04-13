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
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.exceptions import AirflowException

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

    join_financial_news = BigQueryOperator(
    task_id = 'join_financial_news',
    use_legacy_sql = False,
    sql = f'''
            create or replace table `{PROJECT_ID}.{STAGING_DATASET}.join_financial_news` as 
            select distinct * from 
            (select cast(ticker as string) as Ticker, cast(title as string) as Title, cast(date as timestamp) as Date, cast(link as string) as Link, cast(source as string) as Source, cast(comments as string) as Comments
                from `{PROJECT_ID}.{STAGING_DATASET}.sginvestor_news` union distinct
            select cast(ticker as string) as Ticker, cast(title as string) as Title, cast(date as timestamp) as Date, cast(link as string) as Link, cast(source as string) as Source, cast(comments as string) as Comments
                from `{PROJECT_ID}.{STAGING_DATASET}.yahoofinance_news` union distinct
            select cast(ticker as string) as Ticker, cast(title as string) as Title, cast(date as timestamp) as Date, cast(link as string) as Link, cast(source as string) as Source, cast(comments as string) as Comments
                from `{PROJECT_ID}.{STAGING_DATASET}.sginvestor_blog_news` 
            ) temp
    ''',
    dag = dag
    )

    start_transformation = DummyOperator(
        task_id = 'start_transformation',
        dag = dag
    )

    end_transformation = DummyOperator(
        task_id="end_transformation",
        dag=dag
    )
  
    # def force_fail():
    #     raise AirflowException("This error is to test the Postgres task!")
    # # for testing purposes: insert this task after end_transformation, i.e. end_transformation >> force_fail
    # force_fail = PythonOperator(
    #     task_id = 'force_fail',
    #     python_callable = force_fail
    # )


    start_transformation >> join_financial_news >> end_transformation

    return transform_taskgroup