from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

import cchardet
import json
import logging
import os
import pandas as pd
import requests
import urllib.request

logging.basicConfig(level=logging.INFO)

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'


def build_financials_gcs_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup to push extracted data to Google Cloud Storage.
    Parameters
    ----------
    dag: An airflow DAG
    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    financials_gcs_taskgroup = TaskGroup(group_id = 'financials_gcs_tg')

    ############################
    # Push to GCS From XCOMS #
    ############################

    def push_netincome(ti):
        """ Retrieves extracted net income dataframe from XCOMs.
        Pushes the extracted net income dataframe to GoogleCloudStorage in parquet format

        Parameters
        ----------
        ti 
           
        """
        netincome = ti.xcom_pull(task_ids='income_scraping')
        netincome.to_parquet('gs://stock_prediction_is3107/netincome.parquet')
        logging.info('net income parquet file has been pushed to Google Cloud Storage')

    def push_assets(ti):
        """ Retrieves extracted assets dataframe from XCOMs.
        Pushes the extracted assets dataframe to GoogleCloudStorage in parquet format

        Parameters
        ----------
        ti 
           
        """
        assets = ti.xcom_pull(task_ids='assets_scraping')
        assets.to_parquet('gs://stock_prediction_is3107/assets.parquet')
        logging.info('assets parquet file has been pushed to Google Cloud Storage')
    
    def push_liab(ti):
        """ Retrieves extracted liabilities dataframe from XCOMs.
        Pushes the extracted liabilities dataframe to GoogleCloudStorage in parquet format

        Parameters
        ----------
        ti 
           
        """
        liab = ti.xcom_pull(task_ids='liab_scraping')
        liab.to_parquet('gs://stock_prediction_is3107/liab.parquet')
        logging.info('liabilities parquet file has been pushed to Google Cloud Storage')
    
    def push_equity(ti):
        """ Retrieves extracted equity dataframe from XCOMs.
        Pushes the extracted equity dataframe to GoogleCloudStorage in parquet format

        Parameters
        ----------
        ti 
           
        """
        eq = ti.xcom_pull(task_ids='equity_scraping')
        eq.to_parquet('gs://stock_prediction_is3107/equity.parquet')
        logging.info('equity parquet file has been pushed to Google Cloud Storage')
    
    def push_dividends(ti):
        """ Retrieves extracted dividends dataframe from XCOMs.
        Pushes the extracted dividends dataframe to GoogleCloudStorage in parquet format

        Parameters
        ----------
        ti 
           
        """
        div = ti.xcom_pull(task_ids='dividends_scraping')
        div.to_parquet('gs://stock_prediction_is3107/div.parquet')
        logging.info('dividends parquet file has been pushed to Google Cloud Storage')

    def push_inflation(ti):
        """ Retrieves extracted inflation dataframe from XCOMs.
        Pushes the extracted inflation dataframe to GoogleCloudStorage in parquet format

        Parameters
        ----------
        ti 
           
        """
        inflation = ti.xcom_pull(task_ids='inflation_scraping')
        inflation.to_parquet('gs://stock_prediction_is3107/inflation.parquet')
        logging.info('inflation parquet file has been pushed to Google Cloud Storage')


    # Python Operator to push to Google Cloud Storage
    netincome_cloud = PythonOperator(
        task_id = 'netincome_cloud',
        python_callable = push_netincome,
        email_on_failure=True,
        email = 'vickiyew@gmail.com',
        provide_context = True,
        dag = dag
    )

    # Python Operator to push to Google Cloud Storage
    assets_cloud = PythonOperator(
        task_id = 'assets_cloud',
        python_callable = push_assets,
        email_on_failure=True,
        email = 'vickiyew@gmail.com',
        provide_context = True,
        dag = dag
    )

    # Python Operator to push to Google Cloud Storage
    liab_cloud = PythonOperator(
        task_id = 'liab_cloud',
        python_callable = push_liab,
        email_on_failure=True,
        email = 'vickiyew@gmail.com',
        provide_context = True,
        dag = dag
    )

    # Python Operator to push to Google Cloud Storage
    equity_cloud = PythonOperator(
        task_id = 'equity_cloud',
        python_callable = push_equity,
        email_on_failure=True,
        email = 'vickiyew@gmail.com',
        provide_context = True,
        dag = dag
    )

    # Python Operator to push to Google Cloud Storage
    dividends_cloud = PythonOperator(
        task_id = 'dividends_cloud',
        python_callable = push_dividends,
        email_on_failure=True,
        email = 'vickiyew@gmail.com',
        provide_context = True,
        dag = dag
    )

    # Python Operator to push to Google Cloud Storage
    inflation_cloud = PythonOperator(
        task_id = 'inflation_cloud',
        python_callable = push_inflation,
        email_on_failure=True,
        email = 'vickiyew@gmail.com',
        provide_context = True,
        dag = dag
    )

    # TASK DEPENDENCIES

    [netincome_cloud, assets_cloud, liab_cloud, equity_cloud, dividends_cloud, inflation_cloud]

    return financials_gcs_taskgroup