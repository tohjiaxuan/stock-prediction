
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage

import logging
import os
import pandas_gbq

logging.basicConfig(level=logging.INFO)

# Establish connection with staging dataset in BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'

def build_postgres_news_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup for transformation on-premise using Postgresql tables 

    Parameters
    ----------
    dag: An airflow DAG

    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    postgres_news_taskgroup = TaskGroup(group_id = 'postgres_news_taskgroup')

    # Function to execute query (Postgresql)
    def execute_query_with_hook(query):
        """ Executes query using hook

            Parameters
            ----------
            query

        """
        hook = PostgresHook(postgres_conn_id="postgres_local")
        hook.run(query)

    def insert_yahoofinance_table(ti):
        """ Inserts scraped Yahoo Finance news into Postgres table

            Parameters
            ----------
            ti

        """
        yahoofinance_df = ti.xcom_pull(task_ids='yahoofinance_scraping')
        for result in range(len(yahoofinance_df)):
            row = yahoofinance_df.loc[result,:]
            query = f'''    
            INSERT INTO financial_news (Ticker, Title, Date, Link, Source, Comments)
            VALUES ('{row["Ticker"]}', '{row["Title"]}', '{row["Date"]}', '{row["Link"]}', '{row["Source"]}', '{row["Comments"]}');
            '''
            logging.info(f'Query: {query}')
            execute_query_with_hook(query)

    def insert_sginvestor_table(ti):
        """ Inserts scraped SG Investor news into Postgres table

            Parameters
            ----------
            ti

        """
        sginvestor_df = ti.xcom_pull(task_ids='sginvestor_scraping')
        for result in range(len(sginvestor_df)):
            row = sginvestor_df.loc[result,:]
            query = f'''    
            INSERT INTO financial_news (Ticker, Title, Date, Link, Source, Comments)
            VALUES ('{row["Ticker"]}', '{row["Title"]}', '{row["Date"]}', '{row["Link"]}', '{row["Source"]}', '{row["Comments"]}');
            '''
            logging.info(f'Query: {query}')
            execute_query_with_hook(query)

    def insert_sginvestor_blog_table(ti):
        """ Inserts scraped SG Investor Blog news into Postgres table

            Parameters
            ----------
            ti

        """
        sginvestor_blog_df = ti.xcom_pull(task_ids='sginvestor_blog_scraping')
        for result in range(len(sginvestor_blog_df)):
            row = sginvestor_blog_df.loc[result,:]
            query = f'''    
            INSERT INTO financial_news (Ticker, Title, Date, Link, Source, Comments)
            VALUES ('{row["Ticker"]}', '{row["Title"]}', '{row["Date"]}', '{row["Link"]}', '{row["Source"]}', '{row["Comments"]}');
            '''
            logging.info(f'Query: {query}')
            execute_query_with_hook(query)

    def financial_news_bigquery():
        """ Load Postgresql tables into the staging area in BigQuery for loading into the data warehouse in BigQuery.
            First, convert the Postgresql tables into a dataframe with the get_pandas_df helper function.
            Second, directly push the dataframes into the BigQuery staging area.
 
        """
        hook = PostgresHook(postgres_conn_id='postgres_local')
        df = hook.get_pandas_df(sql='select Ticker, Title, Date, Link, Source, Comments from financial_news;')
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.join_financial_news', project_id=PROJECT_ID, if_exists='replace')

    # Create postgres table to store financial news
    create_table_financial_news = PostgresOperator (
    task_id = 'create_table_financial_news',
    dag = dag, 
    postgres_conn_id="postgres_local", 
    sql = '''
        CREATE TABLE IF NOT EXISTS financial_news (
        Id SERIAL PRIMARY KEY,
        Ticker TEXT NOT NULL,
        Title TEXT NOT NULL,
        Date TEXT,
        Link TEXT NOT NULL,
        Source TEXT,
        Comments TEXT
        );
        '''
    )

    # Python operator to insert SG Investor news into table
    insert_sginvestor_table = PythonOperator(
        task_id = 'insert_sginvestor_table',
        python_callable = insert_sginvestor_table,
        dag = dag  
    )

    # Python operator to insert SG Investor Blog news into table
    insert_sginvestor_blog_table = PythonOperator(
        task_id = 'insert_sginvestor_blog_table',
        python_callable = insert_sginvestor_blog_table,
        dag = dag  
    )

    # Python operator to insert Yahoo Finance news into table
    insert_yahoofinance_table = PythonOperator(
        task_id = 'insert_yahoofinance_table',
        python_callable = insert_yahoofinance_table,
        dag = dag  
    )

    # Kickstart on-premise transformation
    start_postgres = DummyOperator(
        task_id = 'start_postgres',
        trigger_rule = 'one_failed', # activate Postgres branch upom failure of transformation taskgroup. 
        dag = dag
    )

    # End on-premise transformation
    end_postgres = BashOperator(
        task_id = 'end_postgres',
        bash_command="echo end_postgres",
        trigger_rule="all_done",
        dag = dag
    )

    # Python operator to push df to BigQuery
    financial_news_bigquery = PythonOperator(
        task_id = 'financial_news_bigquery',
        python_callable = financial_news_bigquery
    )

    start_postgres >> create_table_financial_news >> [insert_yahoofinance_table, insert_sginvestor_table, insert_sginvestor_blog_table] >> financial_news_bigquery >> end_postgres
    return postgres_news_taskgroup







   