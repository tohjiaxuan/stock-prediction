from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, date
from google.cloud import bigquery
from google.cloud import exceptions
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

import argparse
import cchardet
import financials_dwh_schemas as ss
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


def build_financials_schema_taskgroup(dag: DAG) -> TaskGroup: 
    financials_schema_taskgroup = TaskGroup(group_id = 'financials_schema_tg')

    def create_dataset(**kwargs):
        """ Creates dataset in data warehouse 

        Parameters
        ----------
        kwargs**: pass any keyword argument
        
        """
        client = bigquery.Client(project=PROJECT_ID)

        try:
            dataset = client.get_dataset(DWH_DATASET)
        except exceptions.NotFound:
            logging.info(f'Creating dataset {DWH_DATASET}')
            client.create_dataset(DWH_DATASET)
        else:
            logging.info(f'Dataset not created. {DWH_DATASET} already exists.')

    def create_table(**kwargs):
        """ Creates dimension and fact tables in the data warehouse, if they do not exist

        Parameters
        ----------
        kwargs**: pass any keyword argument
        
        """
        client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = client.dataset(DWH_DATASET)
        for table_id in ss.tables:
            schema = eval('ss.'+table_id)
            table_ref = dataset_ref.table(table_id)
            table = bigquery.Table(table_ref, schema=schema)
            if table_id in ['D_INFLATION', 'D_FINANCIALS']:
                try:
                    table = client.create_table(table)
                except exceptions.Conflict:
                    logging.info(f'Table not created. {table_id} already exists')
                else:
                    logging.info(f'Created table {DWH_DATASET}.{table_id}')
            else:
                try:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.MONTH, # partition by month
                        field='Date'  # name of column to use for partitioning
                    )
                    table = client.create_table(table)
                except exceptions.Conflict:
                    logging.info(f'Table not created. {table_id} already exists')
                else:
                    logging.info(f'Created table {DWH_DATASET}.{table_id}')


    create_tables = PythonOperator(
        task_id = 'create_tables',
        python_callable = create_table,
        dag = dag
    )

    

    create_tables
    return financials_schema_taskgroup