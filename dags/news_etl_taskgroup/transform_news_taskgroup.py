from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.models import TaskInstance
from google.cloud import bigquery
from google.cloud import storage
from airflow.utils.task_group import TaskGroup

import os

# Establish connection with staging dataset in BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bq_client = bigquery.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'

def build_transform_news_taskgroup(dag: DAG) -> TaskGroup:
    transform_news_taskgroup = TaskGroup(group_id = 'transform_news_taskgroup')

    join_financial_news = BigQueryOperator(
    task_id = 'join_financial_news',
    use_legacy_sql = False,
    sql = f'''
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.join_financial_news` AS
            SELECT DISTINCT * FROM 
            (SELECT CAST(ticker AS STRING) AS Ticker, CAST(title AS STRING) AS Title, CAST(NULLIF(CAST(date AS STRING), NULL) AS TIMESTAMP) AS Date, CAST(link AS STRING) AS Link, CAST(source AS STRING) AS Source, CAST(comments AS STRING) AS Comments
                FROM `{PROJECT_ID}.{STAGING_DATASET}.sginvestor_news` UNION DISTINCT
            SELECT CAST(ticker AS STRING) AS Ticker, CAST(title AS STRING) AS Title, CAST(NULLIF(CAST(date AS STRING), NULL) AS TIMESTAMP) AS Date, CAST(link AS STRING) AS Link, CAST(source AS STRING) AS Source, CAST(comments AS STRING) AS Comments
                FROM `{PROJECT_ID}.{STAGING_DATASET}.yahoofinance_news` UNION DISTINCT
            SELECT CAST(ticker AS STRING) AS Ticker, CAST(title AS STRING) AS Title, CAST(NULLIF(CAST(date AS STRING), NULL) AS TIMESTAMP) AS Date, CAST(link AS STRING) AS Link, CAST(source AS STRING) AS Source, CAST(comments AS STRING) AS Comments
                FROM `{PROJECT_ID}.{STAGING_DATASET}.sginvestor_blog_news` 
            ) 
    ''',
    dag = dag
    )

    return transform_news_taskgroup