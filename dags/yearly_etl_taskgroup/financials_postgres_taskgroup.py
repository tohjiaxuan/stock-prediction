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
from airflow import AirflowException

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'


def build_financials_postgres_taskgroup(dag: DAG) -> TaskGroup:
    financials_postgres_taskgroup = TaskGroup(group_id = 'financials_postgres_tg')

    # Function to execute query (Postgresql)
    def execute_query_with_hook(query):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        hook.run(query)

    # Functions to insert scraped historical data into their respective Postgresql tables.
    def insert_netincome_init_table(ti):
        netincome_df = ti.xcom_pull(task_ids='income_scraping')
        df_list = netincome_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO netincome_init (ticker, netincome, year2021, year2020, year2019, year2018, year2017)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_assets_init_table(ti):
        assets_df = ti.xcom_pull(task_ids='assets_scraping')
        df_list = assets_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO assets_init (ticker, assets, year2021, year2020, year2019, year2018, year2017)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_liab_init_table(ti):
        liab_df = ti.xcom_pull(task_ids='liab_scraping')
        df_list = liab_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO liab_init (ticker, liability, year2021, year2020, year2019, year2018, year2017)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_equity_init_table(ti):
        eq_df = ti.xcom_pull(task_ids='equity_scraping')
        df_list = eq_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO equity_init (ticker, equity, year2021, year2020, year2019, year2018, year2017)
            VALUES ('{result[0]}', '{result[1].replace("s'", 's')}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_div_init_table(ti):
        div_df = ti.xcom_pull(task_ids='dividends_scraping')
        df_list = div_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO dividends_init (ticker, dividends, year2021, year2020, year2019, year2018, year2017)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_inflation_init_table(ti):
        inflation_df = ti.xcom_pull(task_ids='inflation_scraping')
        df_list = inflation_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO inflation_init (year, inflation)
            VALUES ('{result[0]}', '{result[1]}');
            '''
            print(query)
            execute_query_with_hook(query)

    # Functions to insert scraped up-to-date data into their respective Postgresql tables.
    
    def insert_netincome_yearly_table(ti):
        netincome_df = ti.xcom_pull(task_ids='income_scraping')
        df_list = netincome_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO netincome_yearly (ticker, netincome, prev_year_data)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_assets_yearly_table(ti):
        assets_df = ti.xcom_pull(task_ids='assets_scraping')
        df_list = assets_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO assets_yearly (ticker, assets, prev_year_data)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_liab_yearly_table(ti):
        liab_df = ti.xcom_pull(task_ids='liab_scraping')
        df_list = liab_df.values.tolist()
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO liab_yearly (ticker, liability, prev_year_data)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_equity_yearly_table(ti):
        eq_df = ti.xcom_pull(task_ids='equity_scraping')
        df_list = eq_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO equity_yearly (ticker, equity, prev_year_data)
            VALUES ('{result[0]}', '{result[1].replace("s'", 's')}', '{result[2]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_div_yearly_table(ti):
        div_df = ti.xcom_pull(task_ids='dividends_scraping')
        df_list = div_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO dividends_yearly (ticker, dividends, prev_year_data)
            VALUES ('{result[0]}', '{result[1]}', '{result[2]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_inflation_yearly_table(ti):
        inflation_df = ti.xcom_pull(task_ids='inflation_scraping')
        df_list = inflation_df.values.tolist()
        for result in df_list:
            query = f'''
            INSERT INTO inflation_yearly (year, inflation)
            VALUES ('{result[0]}', '{result[1]}');
            '''
            print(query)
            execute_query_with_hook(query)

    # Airflow operators to insert scraped historical data into their respective Postgresql tables.

    insert_netincome_init_table = PythonOperator(
        task_id = 'insert_netincome_init_table',
        python_callable = insert_netincome_init_table,
        dag = dag
    )

    insert_assets_init_table = PythonOperator(
        task_id = 'insert_assets_init_table',
        python_callable = insert_assets_init_table,
        dag = dag
    )

    insert_liab_init_table = PythonOperator(
        task_id = 'insert_liab_init_table',
        python_callable = insert_liab_init_table,
        dag = dag
    )

    insert_equity_init_table = PythonOperator(
        task_id = 'insert_equity_init_table',
        python_callable = insert_equity_init_table,
        dag = dag
    )

    insert_div_init_table = PythonOperator(
        task_id = 'insert_div_init_table',
        python_callable = insert_div_init_table,
        dag = dag
    )

    insert_inflation_init_table = PythonOperator(
        task_id = 'insert_inflation_init_table',
        python_callable = insert_inflation_init_table,
        dag = dag
    )

    # Airflow operators to insert scraped up-to-date data into their respective Postgresql tables.

    insert_netincome_yearly_table = PythonOperator(
        task_id = 'insert_netincome_yearly_table',
        python_callable = insert_netincome_yearly_table,
        dag = dag
    )

    insert_assets_yearly_table = PythonOperator(
        task_id = 'insert_assets_yearly_table',
        python_callable = insert_assets_yearly_table,
        dag = dag
    )

    insert_liab_yearly_table = PythonOperator(
        task_id = 'insert_liab_yearly_table',
        python_callable = insert_liab_yearly_table,
        dag = dag
    )

    insert_equity_yearly_table = PythonOperator(
        task_id = 'insert_equity_yearly_table',
        python_callable = insert_equity_yearly_table,
        dag = dag
    )

    insert_div_yearly_table = PythonOperator(
        task_id = 'insert_div_yearly_table',
        python_callable = insert_div_yearly_table,
        dag = dag
    )

    insert_inflation_yearly_table = PythonOperator(
        task_id = 'insert_inflation_yearly_table',
        python_callable = insert_inflation_yearly_table,
        dag = dag
    )


    # Airflow operators to create the respective Postgresql tables to store data. 

    create_table_inflation_init = PostgresOperator (
        task_id = 'create_table_inflation_init',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS inflation_init (
            year TEXT NOT NULL PRIMARY KEY,
            inflation REAL
            );
            '''
        )

    create_table_inflation_yearly = PostgresOperator (
        task_id = 'create_table_inflation_yearly',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS inflation_yearly (
            year TEXT NOT NULL PRIMARY KEY,
            inflation REAL
            );
            '''
        )

    create_table_income_init = PostgresOperator (
        task_id = 'create_table_income_init',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS netincome_init (
            ticker TEXT NOT NULL PRIMARY KEY,
            netincome TEXT NOT NULL,
            year2021 REAL,
            year2020 REAL,
            year2019 REAL,
            year2018 REAL,
            year2017 REAL
            );
            '''
        )

    create_table_income_yearly = PostgresOperator (
        task_id = 'create_table_income_yearly',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS netincome_yearly (
            ticker TEXT NOT NULL PRIMARY KEY,
            netincome TEXT NOT NULL,
            prev_year_data REAL
            );
            '''
        )

    create_table_assets_init = PostgresOperator (
        task_id = 'create_table_assets_init',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS assets_init (
            ticker TEXT NOT NULL PRIMARY KEY,
            assets TEXT NOT NULL,
            year2021 REAL,
            year2020 REAL,
            year2019 REAL,
            year2018 REAL,
            year2017 REAL
            );
            '''
        )

    create_table_assets_yearly = PostgresOperator (
        task_id = 'create_table_assets_yearly',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS assets_yearly (
            ticker TEXT NOT NULL PRIMARY KEY,
            assets TEXT NOT NULL,
            prev_year_data REAL
            );
            '''
        )

    create_table_liab_init = PostgresOperator (
        task_id = 'create_table_liab_init',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS liab_init (
            ticker TEXT NOT NULL PRIMARY KEY,
            liability TEXT NOT NULL,
            year2021 REAL,
            year2020 REAL,
            year2019 REAL,
            year2018 REAL,
            year2017 REAL
            );
            '''
        )

    create_table_liab_yearly = PostgresOperator (
        task_id = 'create_table_liab_yearly',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS liab_yearly (
            ticker TEXT NOT NULL PRIMARY KEY,
            liability TEXT NOT NULL,
            prev_year_data REAL
            );
            '''
        )

  
    create_table_equity_init = PostgresOperator (
        task_id = 'create_table_equity_init',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS equity_init (
            ticker TEXT NOT NULL PRIMARY KEY,
            equity TEXT NOT NULL,
            year2021 REAL,
            year2020 REAL,
            year2019 REAL,
            year2018 REAL,
            year2017 REAL
            );
            '''
        )

    create_table_equity_yearly = PostgresOperator (
        task_id = 'create_table_equity_yearly',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS equity_yearly (
            ticker TEXT NOT NULL PRIMARY KEY,
            equity TEXT NOT NULL,
            prev_year_data REAL
            );
            '''
        )

    
    create_table_dividends_init = PostgresOperator (
        task_id = 'create_table_dividends_init',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS dividends_init (
            ticker TEXT NOT NULL PRIMARY KEY,
            dividends TEXT NOT NULL,
            year2021 REAL,
            year2020 REAL,
            year2019 REAL,
            year2018 REAL,
            year2017 REAL
            );
            '''
        )

    create_table_dividends_yearly = PostgresOperator (
        task_id = 'create_table_dividends_yearly',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            CREATE TABLE IF NOT EXISTS dividends_yearly (
            ticker TEXT NOT NULL PRIMARY KEY,
            dividends TEXT NOT NULL,
            prev_year_data REAL
            );
            '''
        )

    # Reformat tables 
    reformat_netincome_init_postgres = PostgresOperator (
        task_id = 'reformat_netincome_init_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
        SELECT * INTO reformat_netincome_init FROM
        (SELECT DISTINCT * FROM ((SELECT ticker, '2021' AS year, year2021 AS netincome FROM netincome_init) 
            UNION ALL   
        (SELECT ticker, '2020' AS year, year2020 AS netincome FROM netincome_init)  
            UNION ALL 
        (SELECT ticker, '2019' AS year, year2019 AS netincome FROM netincome_init)  
            UNION ALL 
        (SELECT ticker, '2018' AS year, year2018 AS netincome FROM netincome_init)  
            UNION ALL 
        (SELECT ticker, '2017' AS year, year2017 AS netincome FROM netincome_init)) AS a) AS b
        '''
    )

    reformat_netincome_yearly_postgres = PostgresOperator (
        task_id = 'reformat_netincome_yearly_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
        SELECT * INTO reformat_netincome_yearly FROM
        (SELECT DISTINCT ticker, to_char(current_date, 'YYYY') AS year, prev_year_data AS netincome FROM netincome_yearly) AS a
        '''
    )

    reformat_assets_init_postgres = PostgresOperator (
        task_id = 'reformat_assets_init_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
        SELECT * INTO reformat_assets_init FROM
        (SELECT DISTINCT * FROM ((SELECT ticker, '2021' AS year, year2021 AS assets FROM assets_init) 
            UNION ALL   
        (SELECT ticker, '2020' AS year, year2020 AS assets FROM assets_init) 
            UNION ALL 
        (SELECT ticker, '2019' AS year, year2019 AS assets FROM assets_init)  
            UNION ALL 
        (SELECT ticker, '2018' AS year, year2018 AS assets FROM assets_init)  
            UNION ALL 
        (SELECT ticker, '2017' AS year, year2017 AS assets FROM assets_init)) AS a) AS b
        '''
    )

    reformat_assets_yearly_postgres = PostgresOperator (
        task_id = 'reformat_assets_yearly_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
        SELECT * INTO reformat_assets_yearly FROM
        (SELECT DISTINCT ticker, to_char(current_date, 'YYYY') AS year, prev_year_data AS assets FROM assets_yearly) AS a
        '''
    )

    reformat_liab_init_postgres = PostgresOperator (
        task_id = 'reformat_liab_init_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
        SELECT * INTO reformat_liab_init FROM
        (SELECT DISTINCT * FROM ((SELECT ticker, '2021' AS year, year2021 AS liability FROM liab_init) 
            UNION ALL  
        (SELECT ticker, '2020' AS year, year2020 AS liability FROM liab_init) 
            UNION ALL 
        (SELECT ticker, '2019' AS year, year2019 AS liability FROM liab_init)  
            UNION ALL 
        (SELECT ticker, '2018' AS year, year2018 AS liability FROM liab_init)  
            UNION ALL 
        (SELECT ticker, '2017' AS year, year2017 AS liability FROM liab_init)) AS a) AS b
        '''
    )

    reformat_liab_yearly_postgres = PostgresOperator (
        task_id = 'reformat_liab_yearly_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
        SELECT * INTO reformat_liab_yearly FROM
        (SELECT DISTINCT ticker, to_char(current_date, 'YYYY') AS year, prev_year_data AS liability FROM liab_yearly) AS a
        '''
    )

    reformat_equity_init_postgres = PostgresOperator (
        task_id = 'reformat_equity_init_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
        SELECT * INTO reformat_equity_init FROM
        (SELECT DISTINCT * FROM ((SELECT ticker, '2021' AS year, year2021 AS equity FROM equity_init) 
            UNION ALL   
        (SELECT ticker, '2020' AS year, year2020 AS equity FROM equity_init) 
            UNION ALL 
        (SELECT ticker, '2019' AS year, year2019 AS equity FROM equity_init)  
            UNION ALL 
        (SELECT ticker, '2018' AS year, year2018 AS equity FROM equity_init)  
            UNION ALL 
        (SELECT ticker, '2017' AS year, year2017 AS equity FROM equity_init)) AS a) AS b
        '''
    )

    reformat_equity_yearly_postgres = PostgresOperator (
        task_id = 'reformat_equity_yearly_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
        SELECT * INTO reformat_equity_yearly FROM
        (SELECT DISTINCT ticker, to_char(current_date, 'YYYY') AS year, prev_year_data AS equity FROM equity_yearly) AS a
        '''
    )

    reformat_dividends_init_postgres = PostgresOperator (
        task_id = 'reformat_dividends_init_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
        SELECT * INTO reformat_dividends_init FROM
        (SELECT DISTINCT * FROM ((SELECT ticker, '2021' AS year, year2021 AS dividends FROM dividends_init) 
            UNION ALL   
        (SELECT ticker, '2020' AS year, year2020 AS dividends FROM dividends_init) 
            UNION ALL 
        (SELECT ticker, '2019' AS year, year2019 AS dividends FROM dividends_init)  
            UNION ALL 
        (SELECT ticker, '2018' AS year, year2018 AS dividends FROM dividends_init) 
            UNION ALL 
        (SELECT ticker, '2017' AS year, year2017 AS dividends FROM dividends_init)) AS a) AS b
        '''
    )

    reformat_dividends_yearly_postgres = PostgresOperator (
        task_id = 'reformat_dividends_yearly_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
        SELECT * INTO reformat_dividends_yearly FROM
        (SELECT DISTINCT ticker, to_char(current_date, 'YYYY') AS year, prev_year_data AS dividends FROM dividends_yearly) AS a
        '''
    )

    # Join the tables together

    join_financials_init_postgres = PostgresOperator (
        task_id = 'join_financials_init_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            SELECT * INTI financials_join_init FROM (
            SELECT iale.ticker, iale.year, iale.netincome, iale.assets, iale.liability, iale.equity, d.dividends
            FROM 
            (SELECT ial.ticker, ial.year, ial.netincome, ial.assets, ial.liability, e.equity
            FROM
            (SELECT ia.ticker, ia.year, ia.netincome, ia.assets, l.liability 
            FROM (SELECT i.ticker, i.year, i.netincome, a.assets FROM reformat_netincome_init i
                    LEFT JOIN reformat_assets_init a 
                            ON i.ticker = a.ticker AND i.year = a.year) AS ia 
                    LEFT JOIN reformat_liab_init l
                            ON ia.ticker = l.ticker AND ia.year = l.year) ial
                    LEFT JOIN reformat_equity_init e
                            ON ial.ticker = e.ticker AND ial.year = e.year) iale
                    LEFT JOIN reformat_dividends_init d
                            ON iale.ticker = d.ticker AND iale.year = d.year
        
            ) AS a
            '''
        )

    join_financials_yearly_postgres = PostgresOperator (
        task_id = 'join_financials_yearly_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            SELECT * INTO financials_join_yearly FROM (
            SELECT iale.ticker, iale.year, iale.netincome, iale.assets, iale.liability, iale.equity, d.dividends
            FROM 
            (SELECT ial.ticker, ial.year, ial.netincome, ial.assets, ial.liability, e.equity
            FROM 
            (SELECT ia.ticker, ia.year, ia.netincome, ia.assets, l.liability 
            FROM (SELECT i.ticker, i.year, i.netincome, a.assets FROM reformat_netincome_yearly i
                    LEFT JOIN reformat_assets_yearly a 
                            ON i.ticker = a.ticker AND i.year = a.year) AS ia 
                    LEFT JOIN reformat_liab_yearly l
                            ON ia.ticker = l.ticker AND ia.year = l.year) ial
                    LEFT JOIN reformat_equity_yearly e
                            ON ial.ticker = e.ticker AND ial.year = e.year) iale
                    LEFT JOIN reformat_dividends_yearly d
                            ON iale.ticker = d.ticker AND iale.year = d.year
        
            ) AS a
            '''
        )

    # Add financial ratios calculations

    financial_ratios_init_postgres = PostgresOperator (
        task_id = 'financial_ratios_init_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            
            ALTER TABLE financials_join_init ADD COLUMN ROA REAL GENERATED ALWAYS AS
                (CASE WHEN NETINCOME = 0 THEN 0
                    WHEN NETINCOME IS NULL THEN NULL
                    WHEN ASSETS IS NULL THEN NULL
                    WHEN ASSETS = 0 THEN NULL
                    ELSE NETINCOME/ASSETS END) STORED;

            ALTER TABLE financials_join_init ADD COLUMN ROE REAL GENERATED ALWAYS AS
                (CASE WHEN NETINCOME = 0 THEN 0
                    WHEN NETINCOME IS NULL THEN NULL
                    WHEN EQUITY = 0 THEN 0
                    WHEN EQUITY IS NULL THEN NULL
                    ELSE NETINCOME/EQUITY END) STORED;

            ALTER TABLE financials_join_init ADD COLUMN DEBT_TO_EQUITY REAL GENERATED ALWAYS AS
                (CASE WHEN LIABILITY = 0 THEN 0
                    WHEN LIABILITY IS NULL THEN NULL
                    WHEN EQUITY = 0 THEN 0
                    WHEN EQUITY IS NULL THEN NULL
                    ELSE LIABILITY/EQUITY END) STORED;

            ALTER TABLE financials_join_init ADD COLUMN NETWORTH REAL GENERATED ALWAYS AS
                (CASE WHEN LIABILITY IS NULL THEN NULL
                    WHEN ASSETS IS NULL THEN NULL 
                    ELSE LIABILITY-ASSETS END) STORED;
                
            UPDATE financials_join_init SET year = concat(year, '-12-31');
            UPDATE financials_join_init SET year = to_timestamp(year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC';
            UPDATE financials_join_init SET ticker = concat(ticker, '.SI');
            
            '''
        )

    financial_ratios_yearly_postgres = PostgresOperator (
        task_id = 'financial_ratios_yearly_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            
            ALTER TABLE financials_join_yearly ADD COLUMN ROA REAL GENERATED ALWAYS AS
                (CASE WHEN NETINCOME = 0 THEN 0
                    WHEN NETINCOME IS NULL THEN NULL
                    WHEN ASSETS IS NULL THEN NULL
                    WHEN ASSETS = 0 THEN NULL
                    ELSE NETINCOME/ASSETS END) STORED;

            ALTER TABLE financials_join_yearly ADD COLUMN ROE REAL GENERATED ALWAYS AS
                (CASE WHEN NETINCOME = 0 THEN 0
                    WHEN NETINCOME IS NULL THEN NULL
                    WHEN EQUITY = 0 THEN 0
                    WHEN EQUITY IS NULL THEN NULL
                    ELSE NETINCOME/EQUITY END) STORED;

            ALTER TABLE financials_join_yearly ADD COLUMN DEBT_TO_EQUITY REAL GENERATED ALWAYS AS
                (CASE WHEN LIABILITY = 0 THEN 0
                    WHEN LIABILITY IS NULL THEN NULL
                    WHEN EQUITY = 0 THEN 0
                    WHEN EQUITY IS NULL THEN NULL
                    ELSE LIABILITY/EQUITY END) STORED;

            ALTER TABLE financials_join_yearly ADD COLUMN NETWORTH REAL GENERATED ALWAYS AS
                (CASE WHEN LIABILITY IS NULL THEN NULL
                    WHEN ASSETS IS NULL THEN NULL 
                    ELSE LIABILITY-ASSETS END) STORED;
            
            UPDATE financials_join_yearly SET year = concat(year, '-12-31');
            UPDATE financials_join_yearly SET year = to_timestamp(year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC';
            UPDATE financials_join_yearly SET ticker = concat(ticker, '.SI');
                
            
            '''
        )

    # reformat table, add unique ID (key) to table
    reformat_financial_ratios_init_postgres = PostgresOperator (
        task_id = 'reformat_financial_ratios_init_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            
            SELECT concat(ticker, '-', extract(year FROM to_timestamp(year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC'), '-', temp.type) AS id,
                temp.ticker, to_timestamp(temp.year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC' as year, 
                temp.type, temp.value into reformat_financials_ratios_init FROM 
                (SELECT * FROM ((SELECT ticker, year, 'netincome' AS type, netincome AS value FROM financials_join_init) 
                UNION ALL
                (SELECT ticker, year, 'assets' AS type, assets AS value FROM financials_join_init)
                UNION ALL 
                (SELECT ticker, year, 'liability' AS type, liability AS value FROM financials_join_init) 
                UNION ALL
                (SELECT ticker, year, 'equity' AS type, equity AS value FROM financials_join_init) 
                UNION ALL 
                (SELECT ticker, year, 'dividends' AS type, dividends AS value FROM financials_join_init)
                UNION ALL 
                (SELECT ticker, year, 'roa' AS type, roa AS value FROM financials_join_init)
                UNION ALL 
                (SELECT ticker, year, 'roe' AS type, roe AS value FROM financials_join_init)
                UNION ALL 
                (SELECT ticker, year, 'debttoequity' AS type, debt_to_equity AS value FROM financials_join_init)
                UNION ALL
                (SELECT ticker, year, 'networth' AS type, networth AS value FROM financials_join_init)) AS a) AS temp;
                
            
            '''
        )

    # reformat table, add unique ID (key) to table
    reformat_financial_ratios_yearly_postgres = PostgresOperator (
        task_id = 'reformat_financial_ratios_yearly_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            
            SELECT concat(ticker, '-', extract(year FROM to_timestamp(year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC'), '-', temp.type) AS id,
                temp.ticker, to_timestamp(temp.year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC' as year, 
                temp.type, temp.value into reformat_financials_ratios_yearly FROM 
                (SELECT * FROM ((SELECT ticker, year, 'netincome' AS type, netincome AS value FROM financials_join_yearly) 
                UNION ALL
                (SELECT ticker, year, 'assets' AS type, assets AS value FROM financials_join_yearly)
                UNION ALL 
                (SELECT ticker, year, 'liability' AS type, liability AS value FROM financials_join_yearly) 
                UNION ALL
                (SELECT ticker, year, 'equity' AS type, equity AS value FROM financials_join_yearly) 
                UNION ALL 
                (SELECT ticker, year, 'dividends' AS type, dividends AS value FROM financials_join_yearly)
                UNION ALL 
                (SELECT ticker, year, 'roa' AS type, roa AS value FROM financials_join_yearly)
                UNION ALL 
                (SELECT ticker, year, 'roe' AS type, roe AS value FROM financials_join_yearly)
                UNION ALL 
                (SELECT ticker, year, 'debttoequity' AS type, debt_to_equity AS value FROM financials_join_yearly)
                UNION ALL
                (SELECT ticker, year, 'networth' AS type, networth AS value FROM financials_join_yearly)) AS a) AS temp;
                
            
            '''
        )


    # add unique ID (key) to table
    inflation_init_key_postgres = PostgresOperator (
        task_id = 'inflation_init_key_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            
            UPDATE inflation_init SET year = concat(year, '-12-31');
            UPDATE inflation_init SET year = to_timestamp(year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC';
            SELECT concat(extract(year FROM to_timestamp(year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC'), '-inflation') as id, to_timestamp(year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC' as year, inflation
            INTO inflation_init_key
            FROM inflation_init;
                        
            
            '''
        )

    # add unique ID (key) to table
    inflation_yearly_key_postgres = PostgresOperator (
        task_id = 'inflation_yearly_key_postgres',
        dag = dag, 
        postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
        sql = '''
            UPDATE inflation_yearly SET year = concat(year, '-12-31');
            UPDATE inflation_yearly SET year = to_timestamp(year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC';
            SELECT concat(extract(year from to_timestamp(year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC'), '-inflation') as id, to_timestamp(year, 'YYYY-MM-DD hh24:mi:ss') at time zone 'Etc/UTC' as year, inflation
            INTO inflation_yearly_key
            FROM inflation_yearly;
                        
            
            '''
        )

    # checking condition - if dimension table in data warehouse is empty, do transformation on the historical data. Otherwise, do transformation on the yearly up-to-date data. 
    def if_d_financials_exists(**kwargs):
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            query = 'select COUNT(*) from `stockprediction-344203.stock_prediction_datawarehouse.D_FINANCIALS`'
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return 'yearly_postgres_financials_transformation'
            else:
                return 'init_postgres_financials_transformation'
        except:
            return 'init_postgres_financials_transformation'


    init_postgres_financials_transformation = DummyOperator(
        task_id = 'init_postgres_financials_transformation',
        dag = dag
    )

    yearly_postgres_financials_transformation = DummyOperator(
        task_id = 'yearly_postgres_financials_transformation',
        dag = dag
    )

    # choose which path to take
    check_financials_choose_transform_path_postgres = BranchPythonOperator(
        task_id = 'check_financials_choose_transform_path_postgres',
        python_callable = if_d_financials_exists,
        do_xcom_push = False,
        dag = dag
    )

    # checking condition - if dimension table in data warehouse is empty, do transformation on the historical data. Otherwise, do transformation on the yearly up-to-date data. 
    def if_d_inflation_exists(**kwargs):
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            query = 'select COUNT(*) from `stockprediction-344203.stock_prediction_datawarehouse.D_INFLATION`'
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return 'yearly_postgres_inflation_transformation'
            else:
                return 'init_postgres_inflation_transformation'
        except:
            return 'init_postgres_inflation_transformation'

    init_postgres_inflation_transformation = DummyOperator(
        task_id = 'init_postgres_inflation_transformation',
        dag = dag
    )

    yearly_postgres_inflation_transformation = DummyOperator(
        task_id = 'yearly_postgres_inflation_transformation',
        dag = dag
    )

    # choose which path to take
    check_inflation_choose_transform_path_postgres = BranchPythonOperator(
        task_id = 'check_inflation_choose_transform_path_postgres',
        python_callable = if_d_inflation_exists,
        do_xcom_push = False,
        dag = dag
    )

    start_transformation_postgres = DummyOperator(
        task_id = 'start_transformation_postgres',
        trigger_rule = 'one_failed', # activate Postgres branch only upon failure of the previous transformation taskgroup. 
        dag = dag
    )

    end_transformation_postgres = BashOperator(
        task_id="end_transformation_postgres",
        bash_command="echo end_transformation_postgres",
        trigger_rule="all_done",
        dag=dag
    )

    # Load Postgresql tables into the staging area in BigQuery for loading into the data warehouse in BigQuery.
    # First, convert the Postgresql tables into a dataframe with the get_pandas_df helper function. 
    # Second, directly push the dataframes into the BigQuery staging area.
    

    def financials_ratios_init_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="select * from reformat_financials_ratios_init;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.reformat_financials_ratios', project_id=PROJECT_ID, if_exists='replace') 
    
    def financials_ratios_yearly_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="select * from reformat_financials_ratios_yearly;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.reformat_financials_ratios', project_id=PROJECT_ID, if_exists='replace') 

    def inflation_key_init_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="select * from inflation_init_key;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.inflation_key', project_id=PROJECT_ID, if_exists='replace')

    def inflation_key_yearly_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="select * from inflation_yearly_key;")
        print(df)
        print(df.dtypes)
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.inflation_key', project_id=PROJECT_ID, if_exists='replace') 


    # airflow operators to push df to BigQuery
    financials_ratios_init_df_bigquery = PythonOperator(
        task_id = 'financials_ratios_init_df_bigquery',
        python_callable = financials_ratios_init_df_bigquery
    )

    financials_ratios_yearly_df_bigquery = PythonOperator(
        task_id = 'financials_ratios_yearly_df_bigquery',
        python_callable = financials_ratios_yearly_df_bigquery
    )

    inflation_key_init_df_bigquery = PythonOperator(
        task_id = 'inflation_key_init_df_bigquery',
        python_callable = inflation_key_init_df_bigquery
    )

    inflation_key_yearly_df_bigquery = PythonOperator(
        task_id = 'inflation_key_yearly_df_bigquery',
        python_callable = inflation_key_yearly_df_bigquery
    )

    
    # TASK DEPENDENCIES
    start_transformation_postgres >> [check_financials_choose_transform_path_postgres, check_inflation_choose_transform_path_postgres]

    check_financials_choose_transform_path_postgres >> [init_postgres_financials_transformation, yearly_postgres_financials_transformation]
    init_postgres_financials_transformation >> [create_table_income_init, create_table_assets_init, create_table_liab_init, create_table_equity_init, create_table_dividends_init]
    create_table_income_init >> insert_netincome_init_table >> reformat_netincome_init_postgres
    create_table_assets_init >> insert_assets_init_table >> reformat_assets_init_postgres
    create_table_liab_init >> insert_liab_init_table >> reformat_liab_init_postgres
    create_table_equity_init >> insert_equity_init_table >> reformat_equity_init_postgres
    create_table_dividends_init >> insert_div_init_table >> reformat_dividends_init_postgres
    [reformat_netincome_init_postgres, reformat_assets_init_postgres, reformat_liab_init_postgres, reformat_equity_init_postgres, reformat_dividends_init_postgres] >> join_financials_init_postgres >> financial_ratios_init_postgres >> reformat_financial_ratios_init_postgres >> financials_ratios_init_df_bigquery


    yearly_postgres_financials_transformation >> [create_table_income_yearly, create_table_assets_yearly, create_table_liab_yearly, create_table_equity_yearly, create_table_dividends_yearly]
    create_table_income_yearly >> insert_netincome_yearly_table >> reformat_netincome_yearly_postgres
    create_table_assets_yearly >> insert_assets_yearly_table >> reformat_assets_yearly_postgres
    create_table_liab_yearly >> insert_liab_yearly_table >> reformat_liab_yearly_postgres
    create_table_equity_yearly >> insert_equity_yearly_table >> reformat_equity_yearly_postgres
    create_table_dividends_yearly >> insert_div_yearly_table >> reformat_dividends_yearly_postgres
    [reformat_netincome_yearly_postgres, reformat_assets_yearly_postgres, reformat_liab_yearly_postgres, reformat_equity_yearly_postgres, reformat_dividends_yearly_postgres] >> join_financials_yearly_postgres >> financial_ratios_yearly_postgres >> reformat_financial_ratios_yearly_postgres >> financials_ratios_yearly_df_bigquery

    check_inflation_choose_transform_path_postgres >> [init_postgres_inflation_transformation, yearly_postgres_inflation_transformation]
    init_postgres_inflation_transformation >> create_table_inflation_init >> insert_inflation_init_table >> inflation_init_key_postgres >> inflation_key_init_df_bigquery
    yearly_postgres_inflation_transformation >> create_table_inflation_yearly >> insert_inflation_yearly_table >> inflation_yearly_key_postgres >> inflation_key_yearly_df_bigquery


    [financials_ratios_init_df_bigquery, financials_ratios_yearly_df_bigquery, inflation_key_init_df_bigquery, inflation_key_yearly_df_bigquery] >> end_transformation_postgres
    
   

    return financials_postgres_taskgroup