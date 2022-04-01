from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage

import pandas as pd
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'

def build_gcs_taskgroup(dag: DAG) -> TaskGroup:
    gcs_taskgroup = TaskGroup(group_id = 'gcs_taskgroup')

    # Push yahoo finance news from XCOM to Cloud
    def push_yahoofinance_news(ti):
        scraping_data = ti.xcom_pull(task_ids='yahoofinance_scraping')
        scraping_data.to_parquet('gs://stock_prediction_is3107/yahoofinance_news.parquet', index = False)
        print("Pushing yahoofinance to Cloud")

    # Push sg investor news from XCOM to Cloud
    def push_sginvestor_news(ti):
        scraping_data = ti.xcom_pull(task_ids='sginvestor_scraping')
        scraping_data.to_parquet('gs://stock_prediction_is3107/sginvestor_news.parquet', index = False)
        print("Pushing sginvestor to Cloud")


    # Push sg investor blog news from XCOM to Cloud
    def push_sginvestor_blog_news(ti):
        scraping_data = ti.xcom_pull(task_ids='sginvestor_blog_scraping')
        scraping_data.to_parquet('gs://stock_prediction_is3107/sginvestor_blog_news.parquet', index = False)
        print("Pushing sginvestor blog to Cloud")

    # # Push business times news from XCOM to Cloud
    # def push_businesstimes_news(ti):
    #     scraping_data = ti.xcom_pull(task_ids='businesstimes_scraping')
    #     scraping_data.to_parquet('gs://stock_prediction_is3107/businesstimes_news.parquet', index = False)
    #     print("Pushing business times to Cloud")

    ####################
    # Pushing to Cloud #
    ####################

    # Push Yahoo Finance to Cloud
    yahoofinance_cloud = PythonOperator(
        task_id = 'yahoofinance_cloud',
        trigger_rule = 'none_failed',
        python_callable = push_yahoofinance_news,
        provide_context = True,
        dag = dag
    )

    # Push Sginvestor to Cloud
    sginvestor_cloud = PythonOperator(
        task_id = 'sginvestor_cloud',
        trigger_rule = 'none_failed',
        python_callable = push_sginvestor_news,
        provide_context = True,
        dag = dag
    )

    # Push Sginvestor Blog to Cloud
    sginvestor_blog_cloud = PythonOperator(
        task_id = 'sginvestor_blog_cloud',
        trigger_rule = 'none_failed',
        python_callable = push_sginvestor_blog_news,
        dag = dag
    )

    # # Push Businesstimes to Cloud
    # businesstimes_cloud = PythonOperator(
    #     task_id = 'businesstimes_cloud',
    #     trigger_rule = 'none_failed',
    #     python_callable = push_businesstimes_news,
    #     provide_context = True,
    #     dag = dag
    # )

    start_gcs = DummyOperator(
        task_id = 'start_gcs',
        dag = dag
    )

    loaded_gcs = DummyOperator(
        task_id = 'loaded_gcs',
        dag = dag
    )

    start_gcs >> [yahoofinance_cloud, sginvestor_cloud, sginvestor_blog_cloud] >> loaded_gcs
# [yahoofinance_cloud, sginvestor_cloud, sginvestor_blog_cloud, businesstimes_cloud]
    return gcs_taskgroup