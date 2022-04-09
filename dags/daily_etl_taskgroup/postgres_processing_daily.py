from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, date
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
import cchardet
import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib.request
import os
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.task_group import TaskGroup
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
import pandas_gbq

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36"}
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bq_client = bigquery.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'

def build_daily_postgres_taskgroup(dag: DAG) -> TaskGroup:
    daily_postgres_taskgroup = TaskGroup(group_id = 'daily_postgres_tg')

    # Function to execute query (Postgresql)
    def execute_query_with_hook(query):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        hook.run(query)

    def insert_stocks_table(ti):     
        stocks_df = ti.xcom_pull(task_ids='stock_scraping_data')
        df_list = stocks_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO stocks_init (Date, Open, High, Low, Close, Volume, Dividends)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_exchange_rates_table(ti):
        exchange_rates_df = ti.xcom_pull(task_ids='exchange_rate_scraping_data')
        df_list = exchange_rates_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO exchange_rates_init (end_of_day, preliminary, eur_sgd, gbp_sgd, usd_sgd, aud_sgd,
            cad_sgd, cny_sgd_100, hkd_sgd_100, inr_sgd_100, idr_sgd_100, jpy_sgd_100, krw_sgd_100, myr_sgd_100, twd_sgd_100,
            nzd_sgd, php_sgd_100, qar_sgd_100, sar_sgd_100, chf_sgd, thb_sgd_100, aed_sgd_100, vnd_sgd_100, timestamp) 
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}','{result[7]}',
            '{result[8]}', '{result[9]}', '{result[10]}', '{result[11]}', '{result[12]}', '{result[13]}', '{result[14]}','{result[15]}',
            '{result[16]}', '{result[17]}', '{result[18]}', '{result[19]}', '{result[20]}', '{result[21]}', '{result[22]}', '{result[23]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_interest_rates_table(ti):
        interest_rates_df = ti.xcom_pull(task_ids='interest_rate_scraping_data')
        df_list = interest_rates_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO interest_rates_init (aggregate_volume, calculation_method, commercial_bills_3m, comp_sora_1m, comp_sora_3m, comp_sora_6m,
            end_of_day, highest_transaction, interbank_12m, interbank_1m, interbank_1w, interbank_2m, interbank_3m, interbank_6m, interbank_overnight,
            lowest_transaction, on_rmb_facility_rate, preliminary, published_date, sgs_repo_overnight_rate, sor_average, sora, sora_index,
            standing_facility_borrow, standing_facility_deposit, usd_sibor_3m, timestamp)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}','{result[7]}',
            '{result[8]}', '{result[9]}', '{result[10]}', '{result[11]}', '{result[12]}', '{result[13]}', '{result[14]}','{result[15]}',
            '{result[16]}', '{result[17]}', '{result[18]}', '{result[19]}', '{result[20]}', '{result[21]}', '{result[22]}', '{result[23]}',
            '{result[24]}', '{result[25]}', '{result[26]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_gold_table(ti):
        gold_df = ti.xcom_pull(task_ids='gold_scraping_data')
        df_list = gold_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO gold_init (Date, Open, High, Low, Close, Adj_Close, Volume)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_silver_table(ti):
        silver_df = ti.xcom_pull(task_ids='silver_scraping_data')
        df_list = silver_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO silver_init (Date, Open, High, Low, Close, Adj_Close, Volume)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_crude_oil_table(ti):
        crude_oil_df = ti.xcom_pull(task_ids='crude_oil_scraping_data')
        df_list = crude_oil_df.values.tolist()
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO crude_oil_init (Date, Open, High, Low, Close, Adj_Close, Volume)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)


    insert_stocks_table = PythonOperator(
        task_id = 'insert_stocks_table',
        python_callable = insert_stocks_table,
        dag = dag
    )

    insert_exchange_rates_table = PythonOperator(
        task_id = 'insert_exchange_rates_table',
        python_callable = insert_exchange_rates_table,
        dag = dag
    )

    insert_interest_rates_table = PythonOperator(
        task_id = 'insert_interest_rates_table',
        python_callable = insert_interest_rates_table,
        dag = dag
    )

    insert_gold_table = PythonOperator(
        task_id = 'insert_gold_table',
        python_callable = insert_gold_table,
        dag = dag
    )

    insert_silver_table = PythonOperator(
        task_id = 'insert_silver_table',
        python_callable = insert_silver_table,
        dag = dag
    )

    insert_crude_oil_table = PythonOperator(
        task_id = 'insert_crude_oil_table',
        python_callable = insert_crude_oil_table,
        dag = dag
    )

    create_stocks_daily_table = PostgresOperator ( 
    task_id = 'create_stocks_daily_table',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS stocks_daily (
        Date TIMESTAMP NOT NULL,
        Open REAL, 
        High REAL,
        Low REAL,
        Close REAL,
        Volume REAL,
        Dividends REAL,
        Stock_Splits INTEGER,
        Stock TEXT NOT NULL PRIMARY KEY
        );
        '''
    )
             
    create_exchange_rates_daily_table = PostgresOperator (
    task_id = 'create_exchange_rates_daily_table',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS exchange_rates_daily (
        end_of_day NOT NULL PRIMARY KEY,
        preliminary TEXT,
        eur_sgd TEXT,
        gbp_sgd TEXT, 
        usd_sgd TEXT,
        aud_sgd TEXT,
        cad_sgd TEXT,
        cny_sgd_100 TEXT,
        hkd_sgd_100	TEXT,
        inr_sgd_100 TEXT,
        idr_sgd_100 TEXT,
        jpy_sgd_100 TEXT,
        krw_sgd_100 TEXT,
        myr_sgd_100 TEXT,
        twd_sgd_100 TEXT,
        nzd_sgd TEXT,
        php_sgd_100 TEXT,
        qar_sgd_100 TEXT,
        sar_sgd_100 TEXT,
        chf_sgd TEXT,
        thb_sgd_100	TEXT,
        aed_sgd_100 TEXT,
        vnd_sgd_100 TEXT,
        timestamp TEXT
        );
        '''
    )
  
    create_interest_rates_daily_table = PostgresOperator (
    task_id = 'create_interest_rates_daily_table',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
            sql = '''
        CREATE TABLE IF NOT EXISTS interest_rates_daily (
        aggregate_volume REAL,
        calculation_method TEXT,
        commercial_bills_3m REAL,
        comp_sora_1m REAL,
        comp_sora_3m REAL,
        comp_sora_6m REAL,
        end_of_day TEXT,
        highest_transaction REAL,
        interbank_12m REAL,
        interbank_1m REAL,
        interbank_1w REAL,
        interbank_2m REAL,
        interbank_3m REAL,
        interbank_6m REAL,
        interbank_overnight REAL,
        lowest_transaction REAL,
        on_rmb_facility_rates TEXT,
        preliminary REAL,
        published_date TEXT,
        sgs_repo_overnight_rates REAL,
        sor_average REAL,
        sora REAL,
        sora_index REAL,
        standing_facility_borrow TEXT,
        standing_facility_deposit TEXT,
        usd_sibor_3m REAL,
        timestamp TEXT NOT NULL PRIMARY KEY
        );
        '''
    )

    create_table_gold_daily = PostgresOperator (
    task_id = 'create_table_gold_daily',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS gold_daily (
        Date TIMESTAMP NOT NULL PRIMARY KEY,
        Open REAL, 
        High REAL,
        Low REAL,
        Close REAL,
        Adj_Close REAL,
        Volume REAL
        );
        '''
    )

    create_table_silver_daily = PostgresOperator (
    task_id = 'create_table_silver_daily',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS silver_daily (
        Date TIMESTAMP NOT NULL PRIMARY KEY,
        Open REAL, 
        High REAL,
        Low REAL,
        Close REAL,
        Adj_Close REAL,
        Volume REAL
        );
        '''
    )

    create_table_crude_oil_daily = PostgresOperator (
    task_id = 'create_table_crude_oil_daily',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS crude_oil_daily (
        Date TIMESTAMP NOT NULL PRIMARY KEY,
        Open REAL, 
        High REAL,
        Low REAL,
        Close REAL,
        Adj_Close REAL,
        Volume REAL
        );
        '''
    )

    create_commodities_daily_table = PostgresOperator (
    task_id = 'create_commodities_daily_table',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS commodities_daily (
        COMM_ID TEXT,
        Date TIMESTAMP NOT NULL PRIMARY KEY,
        Open REAL, 
        High REAL,
        Low REAL,
        Close REAL,
        Adj_Close REAL,
        Volume REAL,
        Price_Category TEXT
        );
        '''
    )

    # create_stocks_daily = PostgresOperator (
    # task_id = 'create_stocks_daily',
    # dag = dag, 
    # postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    # sql = '''
    #     CREATE TABLE IF NOT EXISTS fact_daily (
    #     Date TIMESTAMP NOT NULL,
    #     Open REAL, 
    #     High REAL,
    #     Low REAL,
    #     Close REAL,
    #     Volume REAL,
    #     Dividends REAL,
    #     Stock_Splits INTEGER,
    #     Stock TEXT NOT NULL PRIMARY KEY,
    #     SMA_50 REAL,
    #     SMA_200 REAL,
    #     GC boolean,
    #     DC boolean,
    #     Price_Category TEXT,
    #     usd_sgd REAL,
    #     sora REAL,
    #     networth REAL,
    #     EXR_ID TEXT,
    #     INR_ID TEXT,
    #     FIN_ID TEXT,
    #     INFL_ID TEXT
    #     );
    #     '''
    # )

    distinct_stocks_daily_table = PostgresOperator(
        task_id = 'distinct_stocks_daily_table',
        dag = dag,
        postgres_conn_id = "postgres_local",
        sql = '''
        select * into distinct_stocks_daily from
        (Select distinct * from stocks_daily)
        '''
    )

    distinct_exchange_rates_daily_table = PostgresOperator(
        task_id = 'distinct_exchange_rates_daily_table',
        dag = dag,
        postgres_conn_id = "postgres_local",
        sql = '''
        select distinct parse_timestamp('%Y-%m-%d', end_of_day) as Date,
        concat(temp.end_of_day, '-EXR') as EXR_ID, *
        except (
        end_of_day, preliminary, timestamp
        ),
        preliminary as ex_rate_preliminary, timestamp as ex_rate_timestamp
        into distinct_exchange_rates_daily
        from(Select * from exchange_rates_daily)
        '''
    )

    distinct_interest_rates_daily_table = PostgresOperator(
        task_id = 'distinct_interest_rates_daily_table',
        dag = dag,
        postgres_conn_id = "postgres_local",
        sql = '''
        select distinct parse_timestamp('%Y-%m-%d', end_of_day) as Date,
        concat(temp.end_of_day, '-INR') as INR_ID, *
        except (
            end_of_day, preliminary, timestamp,
            interbank_overnight, interbank_1w, interbank_1m, interbank_2m, interbank_3m,
            interbank_6m, interbank_12m, commercial_bills_3m, usd_sibor_3m, sgs_repo_overnight_rate
        ),
        preliminary as int_rate_preliminary, timestamp as int_rate_timestamp
        into distinct_interest_rates_daily 
        from (Select * from interest_rates_init)
        '''
    )

    distinct_commodities_daily_table = PostgresOperator(
        task_id = 'distinct_commodities_daily_table',
        dag = dag,
        postgres_conn_id = "postgres_local",
        sql = '''
        select distinct concat( FORMAT_TIMESTAMP('%Y-%m-%d', temp.Date) , '-', temp.Price_Category, '-COMM') as COMM_ID,
        * into distinct_commodities_daily from
            (select distinct * from (
                (select "Gold" as Price_Category, * from gold_daily)
                    union all 
                (select "Silver" as Price_Category, * from silver_daily)
                    union all 
                (select "Crude Oil" as Price_Category, * from crude_oil_daily")
            )as a
        ) as b 
        '''
    )

    start_daily_transformation_postgres = DummyOperator(
        task_id = 'start_daily_transformation_postgres',
        dag = dag
    )
    
    end_daily_transformation_postgres = BashOperator(
        task_id="end_daily_transformation_postgres",
        bash_command="echo end_daily_transformation_postgres",
        trigger_rule="all_done",
        dag=dag
    )

    def stocks_daily_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="select * from stocks_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.final_hist_prices', project_id=PROJECT_ID, if_exists='replace') 
    
    def exchange_rates_daily_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="select * from distinct_exchange_rates_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.distinct_exchange_rate', project_id=PROJECT_ID, if_exists='replace') 

    def interest_rates_daily_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="select * from distinct_interest_rates_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.final_interest_rate', project_id=PROJECT_ID, if_exists='replace')

    def commodities_daily_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="select * from distinct_commodities_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.final_commodity_prices', project_id=PROJECT_ID, if_exists='replace') 


    stocks_daily_df_bigquery = PythonOperator(
        task_id = 'stocks_daily_df_bigquery',
        python_callable = stocks_daily_df_bigquery
    )

    exchange_rates_daily_df_bigquery = PythonOperator(
        task_id = 'exchange_rates_daily_df_bigquery',
        python_callable = exchange_rates_daily_df_bigquery
    )

    interest_rates_daily_df_bigquery = PythonOperator(
        task_id = 'interest_rates_daily_df_bigquery',
        python_callable = interest_rates_daily_df_bigquery
    )

    commodities_daily_df_bigquery = PythonOperator(
        task_id = 'commodities_daily_df_bigquery',
        python_callable = commodities_daily_df_bigquery
    )



    start_daily_transformation_postgres >> [create_stocks_daily_table, create_exchange_rates_daily_table, create_interest_rates_daily_table, create_commodities_daily_table] >> create_stocks_daily
    
    create_stocks_daily_table >> insert_stocks_daily_table >> distinct_stocks_daily_table >> stocks_daily_df_bigquery
    create_exchange_rates_daily_table >> insert_exchange_rates_daily_table >> distinct_exchange_rates_daily_table >> exchange_rates_daily_df_bigquery
    create_interest_rates_daily_table >> insert_interest_rates_daily_table >> distinct_interest_rates_daily_table >> interest_rates_daily_df_bigquery
    create_commodities_daily_table >> [create_table_gold_daily, create_table_silver_daily, create_table_crude_oil_daily] >> distinct_commodities_daily_table >> commodities_daily_df_bigquery
    
    create_table_gold_daily >> insert_gold_daily_table 
    create_table_silver_daily >> insert_silver_daily_table 
    create_table_crude_oil_daily >> insert_crude_oil_daily_table

    [stocks_daily_df_bigquery, exchange_rates_daily_df_bigquery, interest_rates_daily_df_bigquery, commodities_daily_df_bigquery] >> end_daily_transformation_postgres


    return daily_postgres_taskgroup