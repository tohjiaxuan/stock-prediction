from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery
from google.cloud import storage

import os

# Establish connection with DWH in BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'
TABLE_ID = 'F_NEWS'

def build_load_news_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup for loading of data from GCS into staging tables

    Parameters
    ----------
    dag: An airflow DAG
    
    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    load_news_taskgroup = TaskGroup(group_id = 'load_news_taskgroup')

    def check_to_load_or_skip(ti):
        """Checks whether to load data into the data warehouse, or to skip loading. 
        Parameters
        ----------
        ti

        Returns
        -------
        task id
           Tells dag which part to take - load data or skip loading
        """
        instruction = ti.xcom_pull(task_ids='dag_path_task')
        if instruction == 'start_gcs_task': 
            return 'f_news_table' 
        else:
            return 'do_not_load'

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

    # BranchPythonOperator tells DAG which path to take
    load_path = BranchPythonOperator(
        task_id = 'load_path',
        python_callable = check_to_load_or_skip,
        do_xcom_push = False,
        provide_context = True,
        dag = dag
    )

    # signal to not load
    do_not_load = BashOperator(
        task_id="do_not_load",
        bash_command="echo do_not_load",
        dag=dag
    )

    # signal the end of task group
    end = BashOperator(
        task_id="end",
        bash_command="echo end",
        trigger_rule="all_done",
        dag=dag
    )

    load_path >> f_news_table 
    do_not_load >> end
    return load_news_taskgroup