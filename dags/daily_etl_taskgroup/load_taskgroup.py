from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
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

def build_load_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup for loading of data from GCS into staging tables

    Parameters
    ----------
    dag: An airflow DAG

    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    load_taskgroup = TaskGroup(group_id = 'load_taskgroup')

    def check_to_load_or_skip(ti):
        """Checks whether to load data into DWH or skip loading
        
        Parameters
        ----------
        ti

        Returns
        -------
        taskid 
            The id that determines whether to load or skip loading
        """
        instruction = ti.xcom_pull(task_ids='dag_path_task')
        if instruction == 'start_gcs_task':
            return 'f_stocks_table'
        else:
            return 'do_not_load'
    
    # BranchPythonOperator tells DAG which path to take
    load_path = BranchPythonOperator(
        task_id = 'load_path',
        python_callable=check_to_load_or_skip,
        do_xcom_push = False,
        provide_context = True,
        dag = dag
    )

    # Create / Append to stock fact table according to sql script
    f_stocks_table = BigQueryExecuteQueryOperator(
        task_id = 'f_stocks_table',
        use_legacy_sql = False,
        params = {
            'project_id': PROJECT_ID,
            'staging_dataset': STAGING_DATASET,
            'dwh_dataset': DWH_DATASET
        },
        destination_dataset_table=f'{PROJECT_ID}:{DWH_DATASET}.F_STOCKS',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        sql = './sql/F_stock.sql',
        dag = dag
    )

    # Create / Append to exchange rate dimension table according to sql script
    d_exchange_table = BigQueryExecuteQueryOperator(
        task_id = 'd_exchange_table',
        use_legacy_sql = False,
        params = {
            'project_id': PROJECT_ID,
            'staging_dataset': STAGING_DATASET,
            'dwh_dataset': DWH_DATASET
        },
        destination_dataset_table=f'{PROJECT_ID}:{DWH_DATASET}.D_EX_RATE',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        sql = './sql/D_exchange_rate.sql',
        dag = dag
    )

    # Create / Append to interest rate dimension table according to sql script
    d_interest_table = BigQueryExecuteQueryOperator(
        task_id = 'd_interest_table',
        use_legacy_sql = False,
        params = {
            'project_id': PROJECT_ID,
            'staging_dataset': STAGING_DATASET,
            'dwh_dataset': DWH_DATASET
        },
        destination_dataset_table=f'{PROJECT_ID}:{DWH_DATASET}.D_INT_RATE',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        sql = './sql/D_interest_rate.sql',
        dag = dag
    )

    # Create / Append to commodities dimension table according to sql script
    d_commodities_table = BigQueryExecuteQueryOperator(
        task_id = 'd_commodities_table',
        use_legacy_sql = False,
        params = {
            'project_id': PROJECT_ID,
            'staging_dataset': STAGING_DATASET,
            'dwh_dataset': DWH_DATASET
        },
        destination_dataset_table=f'{PROJECT_ID}:{DWH_DATASET}.D_COMMODITIES',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        sql = './sql/D_commodities.sql',
        dag = dag
    )

    # Signal to not load
    do_not_load = BashOperator(
        task_id="do_not_load",
        bash_command="echo do_not_load",
        dag = dag
    )

    # End of taskgroup
    end_load = BashOperator(
        task_id = 'end_load_task',
        bash_command="echo end",
        trigger_rule = "all_done",
        dag = dag
    )

    load_path >> [f_stocks_table, do_not_load]
    f_stocks_table >> [d_commodities_table, d_exchange_table, d_interest_table] >> end_load
    do_not_load >> end_load

    return load_taskgroup