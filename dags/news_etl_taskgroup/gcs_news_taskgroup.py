from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage

import logging
import pandas as pd
import os

logging.basicConfig(level=logging.INFO)

# Establish connection with GCS bucket
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'

def build_gcs_news_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup for loading of data into GCS

    Parameters
    ----------
    dag: An airflow DAG

    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    gcs_news_taskgroup = TaskGroup(group_id = 'gcs_news_taskgroup')
        
    ###########################
    # Define Python Functions #
    ###########################

    def push_yahoofinance_news(ti):
        """ Retrieves extracted Yahoo Finance dataframe from XCOMs
        Pushes the extracted Yahoo Finance dataframe to GoogleCloudStorage in parquet format

        Parameters
        ----------
        ti 
           
        """
        scraping_data = ti.xcom_pull(task_ids='yahoofinance_scraping')
        scraping_data.to_parquet('gs://stock_prediction_is3107/yahoofinance_news.parquet', index = False)
        logging.info("Pushing Yahoo Finance to cloud")

    def push_sginvestor_news(ti):
        """ Retrieves extracted SG Investor dataframe from XCOMs
        Pushes the extracted SG Investor dataframe to GoogleCloudStorage in parquet format

        Parameters
        ----------
        ti 
           
        """
        scraping_data = ti.xcom_pull(task_ids='sginvestor_scraping')
        scraping_data.to_parquet('gs://stock_prediction_is3107/sginvestor_news.parquet', index = False)
        logging.info("Pushing SG Investor to cloud")

    def push_sginvestor_blog_news(ti):
        """ Retrieves extracted SG Investor Blog dataframe from XCOMs
        Pushes the extracted SG Investor Blog dataframe to GoogleCloudStorage in parquet format

        Parameters
        ----------
        ti 
           
        """
        scraping_data = ti.xcom_pull(task_ids='sginvestor_blog_scraping')
        scraping_data.to_parquet('gs://stock_prediction_is3107/sginvestor_blog_news.parquet', index = False)
        logging.info("Pushing SG Investor Blog to cloud")

    # Push Yahoo Finance to Cloud
    yahoofinance_cloud = PythonOperator(
        task_id = 'yahoofinance_cloud',
        python_callable = push_yahoofinance_news,
        dag = dag
    )

    # Push SG Investor to Cloud
    sginvestor_cloud = PythonOperator(
        task_id = 'sginvestor_cloud',
        python_callable = push_sginvestor_news,
        dag = dag
    )

    # Push SG Investor Blog to Cloud
    sginvestor_blog_cloud = PythonOperator(
        task_id = 'sginvestor_blog_cloud',
        python_callable = push_sginvestor_blog_news,
        dag = dag
    )

    [yahoofinance_cloud, sginvestor_cloud, sginvestor_blog_cloud]
    return gcs_news_taskgroup