from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage

import logging
import pandas as pd
import os

# Establish connection with GCS bucket
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'

def build_gcs_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup for loading of data into GCS

    Parameters
    ----------
    dag: An airflow DAG

    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    gcs_taskgroup = TaskGroup(group_id = 'gcs_taskgroup')
    
    ###########################
    # Define Python Functions #
    ###########################
    def push_stock_price(**kwargs):
        """Pushes extracted stock prices from XCOMS to GCS bucket

        Parameters
        ----------
        **kwargs: pass any keyword argument
        """
        # Pulls the data from XCOMS by task_ids
        scraping_data = kwargs['ti'].xcom_pull(task_ids='stock_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_stock_scraping_data')

        # Checks if the initialisation stock data exists
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/stock_prices.parquet', engine='pyarrow', index=False)
            logging.info("Pushing historical stock prices to cloud (Initialisation)")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/stock_prices.parquet', engine='pyarrow', index=False)
            logging.info("Pushing historical stock prices to cloud (Update)")

    def push_exchange_rate(**kwargs):
        """Pushes extracted exchange rate from XCOMS to GCS bucket

        Parameters
        ----------
        **kwargs: pass any keyword argument
        """
        # Pulls the data from XCOMS by task_ids
        scraping_data = kwargs['ti'].xcom_pull(task_ids='exchange_rate_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_exchange_rate_scraping_data')

        # Checks if the initialisation exchange rate data exists
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/exchange_rate.parquet', engine='pyarrow', index=False)
            logging.info("Pushing exchange rate to cloud (Initialisation)")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/exchange_rate.parquet', engine='pyarrow', index=False)
            logging.info("Pushing exchange rate to cloud (Update)")
    
    def push_interest_rate(**kwargs):
        """Pushes extracted interest rate from XCOMS to GCS bucket

        Parameters
        ----------
        **kwargs: pass any keyword argument
        """
        # Pulls the data from XCOMS by task_ids
        scraping_data = kwargs['ti'].xcom_pull(task_ids='interest_rate_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_interest_rate_scraping_data')

        # Checks if the initialisation interest rate data exists
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/interest_rate.parquet', engine='pyarrow', index=False)
            logging.info("Pushing interest rate to cloud (Initialisation)")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/interest_rate.parquet', engine='pyarrow', index=False)
            logging.info("Pushing interest rate to cloud (Update)")

    def push_gold(**kwargs):
        """Pushes extracted gold prices from XCOMS to GCS bucket

        Parameters
        ----------
        **kwargs: pass any keyword argument
        """
        # Pulls the data from XCOMS by task_ids
        scraping_data = kwargs['ti'].xcom_pull(task_ids='gold_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_gold_scraping_data')

        # Checks if the initialisation gold data exists
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/gold.parquet', engine='pyarrow', index=False)
            logging.info("Pushing gold prices to cloud (Initialisation)")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/gold.parquet', engine='pyarrow', index=False)
            logging.info("Pushing gold prices to cloud (Update)")

    def push_silver(**kwargs):
        """Pushes extracted silver prices from XCOMS to GCS bucket

        Parameters
        ----------
        **kwargs: pass any keyword argument
        """
        # Pulls the data from XCOMS by task_ids
        scraping_data = kwargs['ti'].xcom_pull(task_ids='silver_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_silver_scraping_data')

        # Checks if the initialisation silver data exists
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/silver.parquet', engine='pyarrow', index=False)
            logging.info("Pushing silver prices to cloud (Initialisation)")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/silver.parquet', engine='pyarrow', index=False)
            logging.info("Pushing silver prices to cloud (Update)")

    def push_crude_oil(**kwargs):
        """Pushes extracted crude oil prices from XCOMS to GCS bucket

        Parameters
        ----------
        **kwargs: pass any keyword argument
        """
        # Pulls the data from XCOMS by task_ids
        scraping_data = kwargs['ti'].xcom_pull(task_ids='crude_oil_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_crude_oil_scraping_data')

        # Checks if the initialisation cruude oil data exists
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/crude_oil.parquet', engine='pyarrow', index=False)
            logging.info("Pushing crude oil prices to cloud (Initialisation)")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/crude_oil.parquet', engine='pyarrow', index=False)
            logging.info("Pushing crude oil prices to cloud (Update)")
    
    #############
    # Operators #
    #############

    # Push Historical Stock Prices to Cloud
    stock_cloud = PythonOperator(
        task_id = 'push_stock_cloud_data',
        python_callable = push_stock_price,
        provide_context = True,
        dag = dag
    )

    # Push Exchange Rates to Cloud
    exchange_cloud = PythonOperator(
        task_id = 'push_exchange_cloud_data',
        python_callable = push_exchange_rate,
        provide_context = True,
        dag = dag
    )

    # Push Interest Rates to Cloud
    interest_cloud = PythonOperator(
        task_id = 'push_interest_cloud_data',
        python_callable = push_interest_rate,
        provide_context = True,
        dag = dag
    )

    # Push Gold to Cloud
    gold_cloud = PythonOperator(
        task_id = 'push_gold_cloud_data',
        python_callable = push_gold,
        provide_context = True,
        dag = dag
    )

    # Push Silver to Cloud
    silver_cloud = PythonOperator(
        task_id = 'push_silver_cloud_data',
        python_callable = push_silver,
        provide_context = True,
        dag = dag
    )

    # Push Crude Oil to Cloud
    crude_oil_cloud = PythonOperator(
        task_id = 'push_crude_oil_cloud_data',
        python_callable = push_crude_oil,
        provide_context = True,
        dag = dag
    )

    [stock_cloud, exchange_cloud, interest_cloud, gold_cloud, silver_cloud, crude_oil_cloud]

    return gcs_taskgroup