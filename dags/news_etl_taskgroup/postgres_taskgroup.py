
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
import pandas_gbq
from airflow.utils.task_group import TaskGroup
from google.cloud import storage

import pandas as pd
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'

def build_postgres_taskgroup(dag: DAG) -> TaskGroup:
    postgres_taskgroup = TaskGroup(group_id = 'postgres_taskgroup')

    # Function to execute query (Postgresql)
    def execute_query_with_hook(query):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        hook.run(query)

    def insert_yahoofinance_table(ti):
        yahoofinance_df = ti.xcom_pull(task_ids='yahoofinance_scraping')
        df_final = yahoofinance_df
        print(df_final)
        for result in range(len(df_final)):
            row = df_final.loc[result,:]
            print('this is row')
            print(row)
            print('this is query')

            query = f'''    
            INSERT INTO financial_news (Ticker, Title, Date, Link, Source, Comments)
            VALUES ('{row["Ticker"]}', '{row["Title"]}', '{row["Date"]}', '{row["Link"]}', '{row["Source"]}', '{row["Comments"]}');
            '''
  
            print(query)
            execute_query_with_hook(query)


    def insert_sginvestor_table(ti):
        sginvestor_df = ti.xcom_pull(task_ids='sginvestor_scraping')
        df_final = sginvestor_df
        # print('sginvestor_df', type(sginvestor_df))
        # print('sginvestor_df.values', type(sginvestor_df.values))
        print(df_final)
        for result in range(len(df_final)):
            row = df_final.loc[result,:]
            print('this is row')
            print(row)
            print('this is query')

            query = f'''    
            INSERT INTO financial_news (Ticker, Title, Date, Link, Source, Comments)
            VALUES ('{row["Ticker"]}', '{row["Title"]}', '{row["Date"]}', '{row["Link"]}', '{row["Source"]}', '{row["Comments"]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_sginvestor_blog_table(ti):
        sginvestor_blog_df = ti.xcom_pull(task_ids='sginvestor_blog_scraping')
        df_final = sginvestor_blog_df

        print(df_final)
        for result in range(len(df_final)):
            row = df_final.loc[result,:]
            print('this is row')
            print(row)
            print('this is query')
            query = f'''    
            INSERT INTO financial_news (Ticker, Title, Date, Link, Source, Comments)
            VALUES ('{row["Ticker"]}', '{row["Title"]}', '{row["Date"]}', '{row["Link"]}', '{row["Source"]}', '{row["Comments"]}');
            '''
            print(query)
            execute_query_with_hook(query)


    # Create postgres table to store financial news
    create_table_financial_news = PostgresOperator (
    task_id = 'create_table_financial_news',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
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

    # dataframe to bq
    def financial_news_bigquery():
        hook = PostgresHook(postgres_conn_id='postgres_local')
        df = hook.get_pandas_df(sql='select Ticker, Title, Date, Link, Source, Comments from financial_news;')
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.join_financial_news', project_id=PROJECT_ID, if_exists='replace')

    insert_sginvestor_table = PythonOperator(
        task_id = 'insert_sginvestor_table',
        python_callable = insert_sginvestor_table,
        dag = dag  
    )

    insert_sginvestor_blog_table = PythonOperator(
        task_id = 'insert_sginvestor_blog_table',
        python_callable = insert_sginvestor_blog_table,
        dag = dag  
    )

    insert_yahoofinance_table = PythonOperator(
        task_id = 'insert_yahoofinance_table',
        python_callable = insert_yahoofinance_table,
        dag = dag  
    )

    start_postgres = DummyOperator(
        task_id = 'start_postgres',
        trigger_rule = 'one_failed', # activate Postgres branch on failure of transformation taskgroup. 
        dag = dag
    )

    end_postgres = DummyOperator(
        task_id = 'end_postgres',
        trigger_rule="all_done",
        dag = dag
    )

    financial_news_bigquery = PythonOperator(
        task_id = 'financial_news_bigquery',
        python_callable = financial_news_bigquery
    )

    start_postgres >> create_table_financial_news >> [insert_yahoofinance_table, insert_sginvestor_table, insert_sginvestor_blog_table] >> financial_news_bigquery >> end_postgres
    return postgres_taskgroup







   