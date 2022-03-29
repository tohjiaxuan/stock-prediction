from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
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

    # def if_f_news_exists():
    #     try:
    #         metadata = bq_client.dataset(DWH_DATASET)
    #         table_ref = metadata.table('F_NEWS')
    #         bq_client.get_table(table_ref)
    #         return True
    #     except:
    #         return False

    # Only create table if does not exist
    f_news_table = BigQueryCreateEmptyTableOperator(
        task_id='f_news_table',
        dataset_id= DWH_DATASET,
        table_id= TABLE_ID,
        project_id= PROJECT_ID,
        schema_fields=[
            {"name": "Ticker", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "Link", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Comments", "type": "STRING", "mode": "NULLABLE"}
        ],
        dag = dag
    )

    # check_f_news_exists = BigQueryTableExistenceSensor(
    #     task_id="check_f_news",
    #     dataset_id= DWH_DATASET,
    #     table_id= TABLE_ID,
    #     project_id= PROJECT_ID,
    #     dag = dag
    # )


    INSERT_ROWS_QUERY = (
    f"INSERT INTO {PROJECT_ID}.{DWH_DATASET}.{TABLE_ID} "
    f"SELECT DISTINCT Ticker, Title, PARSE_DATE('%Y-%m-%d', Date) AS Date, * EXCEPT (Ticker, Title, Date) FROM {PROJECT_ID}.{STAGING_DATASET}.join_financial_news "
    )

    insert_f_news = BigQueryInsertJobOperator(
        task_id='insert_f_news',
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False
        }
        }
    )


    # f_news_table = BigQueryExecuteQueryOperator(
    #     task_id = 'f_news_table',
    #     use_legacy_sql = False,
    #     params = {
    #         'project_id': PROJECT_ID,
    #         'staging_dataset': STAGING_DATASET,
    #         'dwh_dataset': DWH_DATASET
    #     },
    #     destination_dataset_table=f'{PROJECT_ID}:{DWH_DATASET}.F_news',
    #     create_disposition="CREATE_IF_NEEDED",
    #     write_disposition="WRITE_APPEND",
    #     sql = './sql/F_news.sql',
    #     dag = dag
    # )

    end_loading = DummyOperator(
        task_id = 'end_loading',
        dag = dag
    )

    start_loading = DummyOperator(
        task_id = 'start_loading',
        dag = dag
    )

    start_loading >> f_news_table >> insert_f_news >> end_loading

    return load_taskgroup