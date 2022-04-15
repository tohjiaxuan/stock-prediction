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
    daily_postgres_taskgroup = TaskGroup(group_id = 'daily_postgres_taskgroup')

    # Function to execute query (Postgresql)
    def execute_query_with_hook(query):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        hook.run(query)

    def insert_stocks_daily_table(ti):     
        stocks_df = ti.xcom_pull(task_ids='stock_scraping_data')
        df_list = stocks_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO stocks_daily (Date, Open, High, Low, Close, Volume, Dividends, Stock_Splits, Stock)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}', '{result[7]}', '{result[8]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_exchange_rates_daily_table(ti):
        exchange_rates_df = ti.xcom_pull(task_ids='exchange_rate_scraping_data')
        df_list = exchange_rates_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO exchange_rates_daily (end_of_day, preliminary, eur_sgd, gbp_sgd, usd_sgd, aud_sgd,
            cad_sgd, cny_sgd_100, hkd_sgd_100, inr_sgd_100, idr_sgd_100, jpy_sgd_100, krw_sgd_100, myr_sgd_100, twd_sgd_100,
            nzd_sgd, php_sgd_100, qar_sgd_100, sar_sgd_100, chf_sgd, thb_sgd_100, aed_sgd_100, vnd_sgd_100, timestamp) 
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}','{result[7]}',
            '{result[8]}', '{result[9]}', '{result[10]}', '{result[11]}', '{result[12]}', '{result[13]}', '{result[14]}','{result[15]}',
            '{result[16]}', '{result[17]}', '{result[18]}', '{result[19]}', '{result[20]}', '{result[21]}', '{result[22]}', '{result[23]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_interest_rates_daily_table(ti):
        interest_rates_df = ti.xcom_pull(task_ids='interest_rate_scraping_data')
        df_list = interest_rates_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO interest_rates_daily (aggregate_volume, calculation_method, commercial_bills_3m, comp_sora_1m, comp_sora_3m, comp_sora_6m,
            end_of_day, highest_transaction, interbank_12m, interbank_1m, interbank_1w, interbank_2m, interbank_3m, interbank_6m, interbank_overnight,
            lowest_transaction, on_rmb_facility_rate, preliminary, published_date, sgs_repo_overnight_rate, sor_average, sora, sora_index,
            standing_facility_borrow, standing_facility_deposit, usd_sibor_3m, timestamp)
            VALUES ('{'NaN' if result[0] == None else result[0]}', '{'NaN' if result[1] == None else result[1]}', '{'NaN' if result[2] == None else result[2]}',
            '{'NaN' if result[3] == None else result[3]}', '{'NaN' if result[4] == None else result[4]}','{'NaN' if result[5] == None else result[5]}',
            '{'NaN' if result[6] == None else result[6]}','{'NaN' if result[7] == None else result[7]}','{'NaN' if result[8] == None else result[8]}',
            '{'NaN' if result[9] == None else result[9]}', '{'NaN' if result[10] == None else result[10]}', '{'NaN' if result[11] == None else result[11]}',
            '{'NaN' if result[12] == None else result[12]}', '{'NaN' if result[13] == None else result[13]}','{'NaN' if result[14] == None else result[14]}',
            '{'NaN' if result[15] == None else result[15]}', '{'NaN' if result[16] == None else result[16]}', '{'NaN' if result[17] == None else result[17]}',
            '{'NaN' if result[18] == None else result[18]}', '{'NaN' if result[19] == None else result[19]}', '{'NaN' if result[20] == None else result[20]}',
            '{'NaN' if result[21] == None else result[21]}', '{'NaN' if result[22] == None else result[22]}', '{'NaN' if result[23] == None else result[23]}',
            '{'NaN' if result[24] == None else result[24]}', '{'NaN' if result[25] == None else result[25]}', '{'NaN' if result[26] == None else result[26]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_gold_daily_table(ti):
        gold_df = ti.xcom_pull(task_ids='gold_scraping_data')
        df_list = gold_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO gold_daily (Date, Open, High, Low, Close, Adj_Close, Volume)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_silver_daily_table(ti):
        silver_df = ti.xcom_pull(task_ids='silver_scraping_data')
        df_list = silver_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO silver_daily (Date, Open, High, Low, Close, Adj_Close, Volume)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_crude_oil_daily_table(ti):
        crude_oil_df = ti.xcom_pull(task_ids='crude_oil_scraping_data')
        df_list = crude_oil_df.values.tolist()
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO crude_oil_daily (Date, Open, High, Low, Close, Adj_Close, Volume)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)


    insert_stocks_daily_table = PythonOperator(
        task_id = 'insert_stocks_daily_table',
        python_callable = insert_stocks_daily_table,
        dag = dag
    )

    insert_exchange_rates_daily_table = PythonOperator(
        task_id = 'insert_exchange_rates_daily_table',
        python_callable = insert_exchange_rates_daily_table,
        dag = dag
    )

    insert_interest_rates_daily_table = PythonOperator(
        task_id = 'insert_interest_rates_daily_table',
        python_callable = insert_interest_rates_daily_table,
        dag = dag
    )

    insert_gold_daily_table = PythonOperator(
        task_id = 'insert_gold_daily_table',
        python_callable = insert_gold_daily_table,
        dag = dag
    )

    insert_silver_daily_table = PythonOperator(
        task_id = 'insert_silver_daily_table',
        python_callable = insert_silver_daily_table,
        dag = dag
    )

    insert_crude_oil_daily_table = PythonOperator(
        task_id = 'insert_crude_oil_daily_table',
        python_callable = insert_crude_oil_daily_table,
        dag = dag
    )

    create_stocks_daily_table = PostgresOperator ( 
    task_id = 'create_stocks_daily_table',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS stocks_daily (
        Date TIMESTAMP,
        Open REAL, 
        High REAL,
        Low REAL,
        Close REAL,
        Volume REAL,
        Dividends REAL,
        Stock_Splits INTEGER,
        Stock TEXT 
        );
        '''
    )
             
    create_exchange_rates_daily_table = PostgresOperator (
    task_id = 'create_exchange_rates_daily_table',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE exchange_rates_daily (
        end_of_day TEXT,
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
        on_rmb_facility_rate REAL,
        preliminary REAL,
        published_date TEXT,
        sgs_repo_overnight_rate REAL,
        sor_average REAL,
        sora REAL,
        sora_index REAL,
        standing_facility_borrow TEXT,
        standing_facility_deposit TEXT,
        usd_sibor_3m REAL,
        timestamp TEXT
        );
        '''
    )

    create_table_gold_daily = PostgresOperator (
    task_id = 'create_table_gold_daily',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS gold_daily (
        Date TIMESTAMP,
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
        Date TIMESTAMP,
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
        Date TIMESTAMP,
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
        COMM_ID TEXT NOT NULL PRIMARY KEY,
        Date TIMESTAMP,
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

    distinct_stocks_daily_table = PostgresOperator(
        task_id = 'distinct_stocks_daily_table',
        dag = dag,
        postgres_conn_id = "postgres_local",
        sql = '''
        SELECT *
        into distinct_stocks_daily from
        (SELECT DISTINCT * from stocks_daily) as stocks_daily
        '''
    )

    distinct_exchange_rates_daily_table = PostgresOperator(
        task_id = 'distinct_exchange_rates_daily_table',
        dag = dag,
        postgres_conn_id = "postgres_local",
        sql = '''
        SELECT DISTINCT to_timestamp(end_of_day, 'YYYY-MM-DD') as Date,
        concat(end_of_day, '-EXR') as EXR_ID, 
        eur_sgd,
        gbp_sgd,
        usd_sgd, 
        aud_sgd,
        cad_sgd,
        cny_sgd_100,
        hkd_sgd_100,
        inr_sgd_100,
        idr_sgd_100,
        jpy_sgd_100,
        krw_sgd_100,
        myr_sgd_100,
        twd_sgd_100,
        nzd_sgd,
        php_sgd_100,
        qar_sgd_100,
        sar_sgd_100,
        chf_sgd,
        thb_sgd_100,
        aed_sgd_100,
        vnd_sgd_100, 
        preliminary as ex_rate_preliminary, 
        timestamp as ex_rate_timestamp
        into distinct_exchange_rates_daily
        from(SELECT * from exchange_rates_daily) as ex_rates_daily
        '''
    )

    distinct_interest_rates_daily_table = PostgresOperator(
        task_id = 'distinct_interest_rates_daily_table',
        dag = dag,
        postgres_conn_id = "postgres_local",
        sql = '''
        SELECT DISTINCT to_timestamp(end_of_day, 'YYYY-MM-DD') as Date,
        concat(end_of_day, '-INR') as INR_ID, 
        aggregate_volume,
        calculation_method,
        comp_sora_1m,
        comp_sora_3m,
        comp_sora_6m,
        highest_transaction,
        lowest_transaction,
        published_date,
        sor_average,
        sora,
        sora_index,
        standing_facility_borrow,
        standing_facility_deposit,
        preliminary as int_rate_preliminary,
        timestamp as int_rate_timestamp,
        CAST(on_rmb_facility_rate AS TEXT) AS on_rmb_facility_rate
        into distinct_interest_rates_daily 
        from (SELECT * from interest_rates_daily) as ir_daily
        '''
    )

    distinct_commodities_daily_table = PostgresOperator(
        task_id = 'distinct_commodities_daily_table',
        dag = dag,
        postgres_conn_id = "postgres_local",
        sql = '''
        SELECT DISTINCT concat(to_date(cast(Date as TEXT),'YYYY-MM-DD'), '-', Price_Category, '-COMM') as COMM_ID,
        * INTO distinct_commodities_daily FROM
            (SELECT DISTINCT * from (
                (SELECT *, 'Gold' as Price_Category from gold_daily)
                    union all 
                (SELECT *, 'Silver' as Price_Category from silver_daily)
                    union all 
                (SELECT *, 'Crude Oil' as Price_Category from crude_oil_daily)
            )  as a
        ) as b 
        '''  
    )

    start_daily_transformation_postgres = DummyOperator(
        task_id = 'start_daily_transformation_postgres',
        trigger_rule = 'one_failed',
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
        df = hook.get_pandas_df(sql="SELECT * from stocks_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.final_hist_prices', project_id=PROJECT_ID, if_exists='replace') 
    
    def exchange_rates_daily_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="SELECT * from distinct_exchange_rates_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.distinct_exchange_rate', project_id=PROJECT_ID, if_exists='replace') 

    def interest_rates_daily_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="SELECT * from distinct_interest_rates_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.final_interest_rate', project_id=PROJECT_ID, if_exists='replace')

    def commodities_daily_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="SELECT * from distinct_commodities_daily;")
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



    start_daily_transformation_postgres >> [create_stocks_daily_table, create_exchange_rates_daily_table, create_interest_rates_daily_table, create_table_gold_daily, create_table_silver_daily, create_table_crude_oil_daily]
    
    create_stocks_daily_table >> insert_stocks_daily_table >> distinct_stocks_daily_table >> stocks_daily_df_bigquery
    create_exchange_rates_daily_table >> insert_exchange_rates_daily_table >> distinct_exchange_rates_daily_table >> exchange_rates_daily_df_bigquery
    create_interest_rates_daily_table >> insert_interest_rates_daily_table >> distinct_interest_rates_daily_table >> interest_rates_daily_df_bigquery
    create_table_gold_daily >> insert_gold_daily_table 
    create_table_silver_daily >> insert_silver_daily_table 
    create_table_crude_oil_daily >> insert_crude_oil_daily_table

    [insert_gold_daily_table, insert_silver_daily_table, insert_crude_oil_daily_table, create_commodities_daily_table] >> distinct_commodities_daily_table
    distinct_commodities_daily_table >> commodities_daily_df_bigquery
    
    [stocks_daily_df_bigquery, exchange_rates_daily_df_bigquery, interest_rates_daily_df_bigquery, commodities_daily_df_bigquery] >> end_daily_transformation_postgres


    return daily_postgres_taskgroup