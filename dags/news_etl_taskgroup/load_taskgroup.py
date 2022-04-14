from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import bigquery
from google.cloud import storage
from airflow.utils.task_group import TaskGroup
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'
TABLE_ID = 'F_NEWS'

def build_load_taskgroup(dag: DAG) -> TaskGroup:
    load_taskgroup = TaskGroup(group_id = 'load_taskgroup')

    f_news_table = BigQueryExecuteQueryOperator(
        task_id = 'f_news_table',
        use_legacy_sql = False,
        params = {
            'project_id': PROJECT_ID,
            'staging_dataset': STAGING_DATASET,
            'dwh_dataset': DWH_DATASET
        },
        destination_dataset_table=f'{PROJECT_ID}:{DWH_DATASET}.F_NEWS',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        sql = './sql/F_news.sql',
        dag = dag
    )

    return load_taskgroup