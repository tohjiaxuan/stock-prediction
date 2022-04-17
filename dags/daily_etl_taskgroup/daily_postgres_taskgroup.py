from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, date
from google.cloud import bigquery
from google.cloud import storage
# from sqlalchemy import create_engine

import cchardet
import logging
import json
import os
import pandas_gbq
import pandas_ta as ta
import requests
import urllib.request
import pandas as pd
# from sqlalchemy import engine
import psycopg2 as pg
import sqlalchemy

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36"}
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bq_client = bigquery.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'
# engine = pg.connect("dbname='postgres_db' user='postgres_local' host='localhost' port='5432' password='airflow'")
# c_engine = create_engine('postgresql://postgres_local:airflow@localhost:5432/postgres_db')

def build_daily_postgres_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup for transformation on-premise using Postgresql tables. 
    Parameters
    ----------
    dag: An airflow DAG
    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    daily_postgres_taskgroup = TaskGroup(group_id = 'daily_postgres_taskgroup')

    # Function to execute query (Postgresql)
    def execute_query_with_hook(query):
        """ Executes query using hook

        Parameters
        ----------
        query

        """
        hook = PostgresHook(postgres_conn_id="postgres_local")
        hook.run(query)

    def insert_stocks_daily_table(ti):   
        """ Inserts scraped stocks daily into Postgres table

        Parameters
        ----------
        ti

        """  
        stocks_df = ti.xcom_pull(task_ids='stock_scraping_data')
        df_list = stocks_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO stocks_daily (Date, Open, High, Low, Close, Volume, Dividends, Stock_Splits, Stock)
            VALUES ('{'NaN' if result[0] == None else result[0]}', '{'NaN' if result[1] == None else result[1]}', '{'NaN' if result[2] == None else result[2]}',
             '{'NaN' if result[3] == None else result[3]}', '{'NaN' if result[4] == None else result[4]}', '{'NaN' if result[5] == None else result[5]}',
              '{'NaN' if result[6] == None else result[6]}', '{'NaN' if result[7] == None else result[7]}', '{'NaN' if result[0] == None else result[8]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_exchange_rates_daily_table(ti):
        """ Inserts scraped exchange rates daily into Postgres table

        Parameters
        ----------
        ti

        """  
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
            VALUES ('{'NaN' if result[0] == None else result[0]}', '{'NaN' if result[1] == None else result[1]}', '{'NaN' if result[2] == None else result[2]}',
            '{'NaN' if result[3] == None else result[3]}', '{'NaN' if result[4] == None else result[4]}','{'NaN' if result[5] == None else result[5]}',
            '{'NaN' if result[6] == None else result[6]}','{'NaN' if result[7] == None else result[7]}','{'NaN' if result[8] == None else result[8]}',
            '{'NaN' if result[9] == None else result[9]}', '{'NaN' if result[10] == None else result[10]}', '{'NaN' if result[11] == None else result[11]}',
            '{'NaN' if result[12] == None else result[12]}', '{'NaN' if result[13] == None else result[13]}','{'NaN' if result[14] == None else result[14]}',
            '{'NaN' if result[15] == None else result[15]}', '{'NaN' if result[16] == None else result[16]}', '{'NaN' if result[17] == None else result[17]}',
            '{'NaN' if result[18] == None else result[18]}', '{'NaN' if result[19] == None else result[19]}', '{'NaN' if result[20] == None else result[20]}',
            '{'NaN' if result[21] == None else result[21]}', '{'NaN' if result[22] == None else result[22]}', '{'NaN' if result[23] == None else result[23]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_interest_rates_daily_table(ti):
        """ Inserts scraped interest rates daily into Postgres table

        Parameters
        ----------
        ti

        """  
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
        """ Inserts scraped gold daily into Postgres table

        Parameters
        ----------
        ti

        """  
        gold_df = ti.xcom_pull(task_ids='gold_scraping_data')
        df_list = gold_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO gold_daily (Date, Open, High, Low, Close, Adj_Close, Volume)
            VALUES ('{'NaN' if result[0] == None else result[0]}', '{'NaN' if result[1] == None else result[1]}', '{'NaN' if result[2] == None else result[2]}',
            '{'NaN' if result[3] == None else result[3]}', '{'NaN' if result[4] == None else result[4]}','{'NaN' if result[5] == None else result[5]}',
            '{'NaN' if result[6] == None else result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)
    
    def insert_silver_daily_table(ti):
        """ Inserts scraped silver daily into Postgres table

        Parameters
        ----------
        ti

        """  
        silver_df = ti.xcom_pull(task_ids='silver_scraping_data')
        df_list = silver_df.values.tolist()
        print(df_list)
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO silver_daily (Date, Open, High, Low, Close, Adj_Close, Volume)
            VALUES ('{'NaN' if result[0] == None else result[0]}', '{'NaN' if result[1] == None else result[1]}', '{'NaN' if result[2] == None else result[2]}',
            '{'NaN' if result[3] == None else result[3]}', '{'NaN' if result[4] == None else result[4]}','{'NaN' if result[5] == None else result[5]}',
            '{'NaN' if result[6] == None else result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)

    def insert_crude_oil_daily_table(ti):
        """ Inserts scraped crude_oil daily into Postgres table

        Parameters
        ----------
        ti

        """  
        crude_oil_df = ti.xcom_pull(task_ids='crude_oil_scraping_data')
        df_list = crude_oil_df.values.tolist()
        for result in df_list:
            print('this is result')
            print(result)
            print('this is query')
            query = f'''
            INSERT INTO crude_oil_daily (Date, Open, High, Low, Close, Adj_Close, Volume)
            VALUES ('{'NaN' if result[0] == None else result[0]}', '{'NaN' if result[1] == None else result[1]}', '{'NaN' if result[2] == None else result[2]}',
            '{'NaN' if result[3] == None else result[3]}', '{'NaN' if result[4] == None else result[4]}','{'NaN' if result[5] == None else result[5]}',
            '{'NaN' if result[6] == None else result[6]}');
            '''
            print(query)
            execute_query_with_hook(query)

    ############################
    #Python Functions to obtain  #
    ############################

    def if_f_stock_exists():
        """Check if Stocks Fact Table is present in DWHS

        Returns
        -------
        bool
        """
        try:
            # Establish connection with BigQuery
            bq_client = bigquery.Client()
            query = 'SELECT COUNT(`Date`) FROM `stockprediction-344203.stock_prediction_datawarehouse.F_STOCKS`'
            
            # Convert queried result to df
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return True
            else:
                return False
        except:
            return False
    
    def query_stock_dwh():
        """Query stocks data from dwh for technical analysis

        Returns
        -------
        dataframe
            Contains the historical stocks data for the past 200 days for 30 stocks
        """
        # Establish connection with DWH
        bq_client = bigquery.Client()
        
        # Query 6000 rows because we require 200 days worth of past data for each stock
        # Value can change depending on the number of days for SMA / stock (30*200)
        query = """SELECT Date, Open, High, Low, Close, Volume, Dividends, Stock_Splits, Stock
        FROM `stockprediction-344203.stock_prediction_datawarehouse.F_STOCKS` ORDER BY `Date` DESC LIMIT 6000"""
        df = bq_client.query(query).to_dataframe()
        return df

    def helper_sma_prices(input_df):
        """Helper function to obtain the sma and golden, death crosess for each stock
        
        Parameters
        ----------
        input_df: dataframe
            Historical stock prices

        Returns
        -------
        dataframe
            Contains historical prices, sma50, sma200, golden and death crosses
        """
        # Obtain unique values
        uniq_stocks = input_df['Stock'].unique()
        
        sma_stock_postgress = []
        # Check for null values, if present, do ffill
        for ticker in uniq_stocks:
            curr_df = input_df.loc[input_df['Stock'] == ticker]

            # Sort by date
            curr_df = curr_df.sort_values(by = ['Date'])
            curr_df = curr_df.reset_index(drop = True)

            # Forward fill values
            if (curr_df.isnull().values.any() == True):
                curr_df = curr_df.fillna(method='ffill')
                    
            # Conduct TA analysis
            curr_df['SMA_50'] = ta.sma(curr_df['Close'], length = 50, append=True)
            curr_df['SMA_200'] = ta.sma(curr_df['Close'], length=200, append=True)
            curr_df["GC"] = ta.sma(curr_df['Close'], length = 50, append=True) > ta.sma(curr_df['Close'], length=200, append=True)
            curr_df["DC"] = ta.sma(curr_df['Close'], length = 50, append=True) > ta.sma(curr_df['Close'], length=200, append=True)
            sma_stock_postgress.append(curr_df)
        
        # Transform to df
        final_df = pd.concat(sma_stock_postgress, ignore_index = True)

        # Add Constant column for fact table later on
        final_df['Price Category'] = "Stock"
            
        logging.info('Completed technical analysis for stocks')
        return final_df

    def get_distinct_stocks_daily_df():
        """Convert distinct_stocks_daily table to dataframe
        Returns
        -------
        dataframe
            Contains the distinct historical stocks data 
        """
        # distinct_stocks_daily_df = pd.read_sql_query('select * from distinct_stocks_daily', con=engine)
        
        hook = PostgresHook(postgres_conn_id="postgres_local")
        distinct_stocks_daily_df = hook.get_pandas_df(sql="SELECT * from distinct_stocks_daily;")
        logging.info('distinct_stocks_daily dataframe')
        distinct_stocks_daily_df.rename(columns={'date':'Date'}, inplace=True)
        return distinct_stocks_daily_df
    
    def get_distinct_interest_rates_daily_df():
        """Convert distinct_interest_rates_daily table to dataframe
        Returns
        -------
        dataframe
            Contains the distinct historical interest rate data 
        """
        # distinct_interest_rates_daily_df = pd.read_sql_query('select * from distinct_interest_rates_daily', con = engine)
        hook = PostgresHook(postgres_conn_id="postgres_local")
        distinct_interest_rates_daily_df = hook.get_pandas_df(sql="SELECT * from distinct_interest_rates_daily;")
        # print(distinct_interest_rates_daily_df)
        logging.info('distinct_interest_rates_daily dataframe')
    
        # To prepare df for lagging dates, change column name
        distinct_interest_rates_daily_df.rename(columns={'date':'Actual Date'}, inplace=True)

        return distinct_interest_rates_daily_df
    
    def get_distinct_exchange_rates_daily_df():
        """Retrive historical date and convert it to dataframe

        Returns
        -------
        dataframe
            Contains the distinct date from historical exchaNge rate data from staging tables
        """
        # distinct_exchange_rates_daily_df = pd.read_sql_query('select date from distinct_exchange_rates_daily order by date desc', con=engine)
        hook = PostgresHook(postgres_conn_id="postgres_local")
        distinct_exchange_rates_daily_df = hook.get_pandas_df(sql="SELECT Date from distinct_exchange_rates_daily order by date desc;")
        logging.info('distinct_exchange_rates_daily dataframe')

        distinct_exchange_rates_daily_df.rename(columns={'date':'Date'}, inplace=True)
        return distinct_exchange_rates_daily_df

    def update_sma():
        """Helper function to obtain sma and golden, death crosess for updated stock data

        Returns
        -------
        dataframe
            Contains historical prices, sma50, sma200, golden and death crosses for updated stock
        """
        new_df = get_distinct_stocks_daily_df()
        old_df = query_stock_dwh()

        # Create new dataframe that contains the past 200 days of data and the newly updated stock data
        df = old_df.append(new_df, ignore_index = True)

        # Get unique dates to prevent duplicated data
        uniq_dates = new_df['Date'].unique()
        output = helper_sma_prices(df) # Calculate SMA, golden and death crosses
        output = output[output['Date'].isin(uniq_dates)] # Only keep records that are present in updated stock data
        return output

    def sma_prices():
        """Obtain sma and golden, death crosess for stock data
        """
        check_dwh = if_f_stock_exists() # Check if fact table exists
        # If it exists, then we just have to update SMA instead of populating for everying
        if check_dwh:
            logging.info('Start to update SMA, GC and DC for updated stock data')
            results = update_sma()
            logging.info('Completed SMA, GC and DC update')
        else:
            logging.info('Start to initialise SMA, GC and DC')
            temp = get_distinct_stocks_daily_df()
            results = helper_sma_prices(temp)
            logging.info('Completed SMA, GC and DC initialisation')
        
        #Create final_stock postgres table
        for result in results:
            print('this is result')
            print(len(result))
            print(result)
            print('this is query')
            query = f'''    
                INSERT INTO final_stock (Date, Open, High, Low, Close, Volume, Dividends, Stock_Splits, Stock, SMA_50, SMA_200, GC, DC, Price_Category)
                VALUES 
                ('{'NaN' if result[0] == None else result[0]}', '{'NaN' if result[1] == None else result[1]}', '{'NaN' if result[2] == None else result[2]}',
                '{'NaN' if result[3] == None else result[3]}', '{'NaN' if result[4] == None else result[4]}','{'NaN' if result[5] == None else result[5]}',
                '{'NaN' if result[6] == None else result[6]}','{'NaN' if result[7] == None else result[7]}','{'NaN' if result[8] == None else result[8]}',
                '{'NaN' if result[9] == None else result[9]}', '{'NaN' if result[10] == None else result[10]}', '{'NaN' if result[11] == None else result[11]}',
                '{'NaN' if result[12] == None else result[12]}', '{'NaN' if result[13] == None else result[13]}');
                '''
        													
            logging.info(f'Query: {query}')
            execute_query_with_hook(query)
            # result.to_sql('final_stock', c_engine)
    
    def lag_int_postgres():
        """Obtain lagged date for interest rate
        """
        int_df = get_distinct_interest_rates_daily_df() 
        ex_df = get_distinct_exchange_rates_daily_df() # Lagged dates are obtained from exchange rate

        # To ensure index is the same
        int_df = int_df.reset_index(drop=True)
        ex_df = ex_df.reset_index(drop=True)

        dates =  ex_df['Date'].to_list()
        print(int_df)
        
        # If interest date has more data, we start from first row
        if (len(ex_df) < len(int_df)):
            int_df = int_df.iloc[1:]
        
        results = int_df
        results['Date'] = dates

        # Rename columns to ensure consistency
        if 'date' in results.columns and 'inr_id' in results.columns:
            results.rename(columns={'date': 'Date', 'inr_id': 'INR_ID'}, inplace=True)    

        logging.info("Create final_interest_rate postgres table")

        for result in results :
            print('this is result')
            print(len(result))
            print(result)
            print('this is query')												
            query = f'''    
            INSERT INTO final_interest_rate (Actual_Date, INR_ID, aggregate_volume, calculation_method, comp_sora_1m, comp_sora_3m, comp_sora_6m,
            highest_transaction, lowest_transaction, published_date, sor_average, sora, sora_index, standing_facility_borrow, standing_facility_deposit,
            int_rate_preliminary, int_rate_timestamp, on_rmb_facility_rate, Date)
            VALUES('{'NaN' if result[0] == None else result[0]}', '{'NaN' if result[1] == None else result[1]}', '{'NaN' if result[2] == None else result[2]}',
            '{'NaN' if result[3] == None else result[3]}', '{'NaN' if result[4] == None else result[4]}','{'NaN' if result[5] == None else result[5]}',
            '{'NaN' if result[6] == None else result[6]}','{'NaN' if result[7] == None else result[7]}','{'NaN' if result[8] == None else result[8]}',
            '{'NaN' if result[9] == None else result[9]}', '{'NaN' if result[10] == None else result[10]}', '{'NaN' if result[11] == None else result[11]}',
            '{'NaN' if result[12] == None else result[12]}', '{'NaN' if result[13] == None else result[13]}','{'NaN' if result[14] == None else result[14]}',
            '{'NaN' if result[15] == None else result[15]}', '{'NaN' if result[16] == None else result[16]}', '{'NaN' if result[17] == None else result[17]}');
            '''
            logging.info(f'Query: {query}')
            execute_query_with_hook(query)

    # Python operator to insert daily stocks into table
    insert_stocks_daily_table = PythonOperator(
        task_id = 'insert_stocks_daily_table',
        python_callable = insert_stocks_daily_table,
        dag = dag
    )

    # Python operator to insert daily exchange rates into table
    insert_exchange_rates_daily_table = PythonOperator(
        task_id = 'insert_exchange_rates_daily_table',
        python_callable = insert_exchange_rates_daily_table,
        dag = dag
    )

    # Python operator to insert daily interest rates into table
    insert_interest_rates_daily_table = PythonOperator(
        task_id = 'insert_interest_rates_daily_table',
        python_callable = insert_interest_rates_daily_table,
        dag = dag
    )

    # Python operator to insert daily gold into table
    insert_gold_daily_table = PythonOperator(
        task_id = 'insert_gold_daily_table',
        python_callable = insert_gold_daily_table,
        dag = dag
    )

    # Python operator to insert daily silver into table
    insert_silver_daily_table = PythonOperator(
        task_id = 'insert_silver_daily_table',
        python_callable = insert_silver_daily_table,
        dag = dag
    )

    # Python operator to insert daily crude oil into table
    insert_crude_oil_daily_table = PythonOperator(
        task_id = 'insert_crude_oil_daily_table',
        python_callable = insert_crude_oil_daily_table,
        dag = dag
    )
    
    # Python operator to create Postgres table for daily stocks
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
    
    # Python operator to create Postgres table for daily exchange rates    
    create_exchange_rates_daily_table = PostgresOperator (
    task_id = 'create_exchange_rates_daily_table',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS exchange_rates_daily (
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
    
    # Python operator to create Postgres table for daily interest rates    
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

    # Python operator to create Postgres table for daily gold
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

    # Python operator to create Postgres table for daily silver
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
    
    # Python operator to create Postgres table for daily crude oil
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
    # Python operator to create Postgres table for daily commodities
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
    
    # create_distinct_exchange_rates_daily_table = PostgresOperator (
    # task_id = 'create_distinct_exchange_rates_daily_table',
    # dag = dag, 
    # postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    # sql = '''
    #     CREATE TABLE IF NOT EXISTS distinct_exchange_rates_daily (
    #     Date TIMESTAMP,
    #     EXR_ID TEXT, 
    #     eur_sgd TEXT,
    #     gbp_sgd TEXT,
    #     usd_sgd TEXT, 
    #     aud_sgd TEXT,
    #     cad_sgd TEXT,
    #     cny_sgd_100 TEXT,
    #     hkd_sgd_100 TEXT,
    #     inr_sgd_100 TEXT,
    #     idr_sgd_100 TEXT,
    #     jpy_sgd_100 TEXT,
    #     krw_sgd_100 TEXT,
    #     myr_sgd_100 TEXT,
    #     twd_sgd_100 TEXT,
    #     nzd_sgd TEXT,
    #     php_sgd_100 TEXT,
    #     qar_sgd_100 TEXT,
    #     sar_sgd_100 TEXT,
    #     chf_sgd TEXT,
    #     thb_sgd_100 TEXT,
    #     aed_sgd_100 TEXT,
    #     vnd_sgd_100 TEXT, 
    #     ex_rate_preliminary TEXT, 
    #     ex_rate_timestamp TEXT
    #     );
    #     '''
    # )

    #     # Python operator to create Postgres table for daily distinct stocks
    # create_distinct_stock_daily_table = PostgresOperator ( 
    # task_id = 'create_distinct_stock_daily_table',
    # dag = dag, 
    # postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    # sql = '''
    #     CREATE TABLE IF NOT EXISTS distinct_stocks_daily (
    #     Date TIMESTAMP,
    #     Open REAL, 
    #     High REAL,
    #     Low REAL,
    #     Close REAL,
    #     Volume REAL,
    #     Dividends REAL,
    #     Stock_Splits INTEGER,
    #     Stock TEXT 
    #     );
    #     '''
    # )

    # Python operator to create Postgres table for daily final stocks
    create_final_stock_table = PostgresOperator (
    task_id = 'create_final_stock_table',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS final_stock (
        Date TIMESTAMP,
        Open double precision, 
        High double precision,
        Low double precision,
        Close double precision,
        Volume double precision,
        Dividends double precision,
        Stock_Splits integer,
        Stock TEXT,
        SMA_50 double precision,
        SMA_200 double precision,
        GC BOOLEAN,
        DC BOOLEAN,
        Price_Category TEXT
        );
        '''
    )

    # Python operator to create Postgres table for daily final interest rate
    create_final_interest_rate = PostgresOperator (
    task_id = 'create_final_interest_rate',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS final_interest_rate (
        Actual_Date TIMESTAMP,
        INR_ID TEXT,
        aggregate_volume double precision, 
        calculation_method TEXT,
        comp_sora_1m double precision,
        comp_sora_3m double precision,
        comp_sora_6m double precision,
        highest_transaction double precision,
        lowest_transaction double precision,
        published_date TEXT,
        sor_average double precision,
        sora double precision,	
        sora_index double precision,	
        standing_facility_borrow TEXT,	
        standing_facility_deposit TEXT,	
        int_rate_preliminary integer, 	 
        int_rate_timestamp TEXT,	
        on_rmb_facility_rate integer,	
        Date TIMESTAMP
        );
        '''
    )


    # Python operator to create Postgres table for daily distinct stocks
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

    cast_interest_rate_daily_table = PostgresOperator(
        task_id = 'cast_interest_rate_daily_table',
        dag = dag,
        postgres_conn_id = "postgres_local",
        sql = '''
        SELECT DISTINCT to_timestamp(end_of_day, 'YYYY-MM-DD') as Date,
        concat(end_of_day, '-INR') as INR_ID, 
        CAST(aggregate_volume AS double precision) AS aggregate_volume,
        CAST(calculation_method AS TEXT) AS calculation_method,
        CAST(comp_sora_1m AS double precision) AS comp_sora_1m,
        CAST(comp_sora_3m AS double precision) AS comp_sora_3m,
        CAST(comp_sora_6m AS double precision) AS comp_sora_6m,
        CAST(highest_transaction AS double precision) AS highest_transaction,
        CAST(lowest_transaction AS double precision) AS lowest_transaction,
        CAST(on_rmb_facility_rate AS TEXT) AS on_rmb_facility_rate,
        CAST(published_date AS TEXT) AS published_date,
        CAST(sor_average AS double precision) AS sor_average,
        CAST(sora AS double precision) AS sora,
        CAST(sora_index AS double precision) AS sora_index,
        CAST(standing_facility_borrow AS TEXT) AS standing_facility_borrow,
        CAST(standing_facility_deposit AS TEXT) AS standing_facility_deposit,
        CAST(int_rate_preliminary AS integer) AS int_rate_preliminary, 
        CAST(int_rate_timestamp AS TEXT) AS int_rate_timestamp,
        into cast_interest_rate_daily
        from (SELECT * from final_interest_rate) as ir_daily
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
        df = hook.get_pandas_df(sql="SELECT * from final_stock;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.final_hist_prices', project_id=PROJECT_ID, if_exists='replace') 
    
    def exchange_rates_daily_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="SELECT * from distinct_exchange_rates_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.distinct_exchange_rate', project_id=PROJECT_ID, if_exists='replace') 

    def distinct_interest_rates_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="SELECT * from distinct_interest_rates_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.distinct_interest_rate', project_id=PROJECT_ID, if_exists='replace')

    def interest_rates_daily_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="SELECT * from final_interest_rate;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.final_interest_rate', project_id=PROJECT_ID, if_exists='replace')

    def cast_int_rate_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="SELECT * from cast_interest_rate_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.casted_interest_rate', project_id=PROJECT_ID, if_exists='replace') 

    def commodities_daily_df_bigquery(**kwargs):
        hook = PostgresHook(postgres_conn_id="postgres_local")
        df = hook.get_pandas_df(sql="SELECT * from distinct_commodities_daily;")
        pandas_gbq.to_gbq(df, 'stock_prediction_staging_dataset.final_commodity_prices', project_id=PROJECT_ID, if_exists='replace') 

    # Add SMA to df
    sma_stock_postgres = PythonOperator(
        task_id = 'sma_stock_postgres_task',
        python_callable = sma_prices,
        dag = dag
    )

    # Add lag dates to df
    lag_int_postgres = PythonOperator(
        task_id = 'lag_int_postgres_task',
        python_callable = lag_int_postgres,
        dag = dag
    )   

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

    distinct_interest_rates_df_bigquery = PythonOperator(
        task_id = 'distinct_interest_rates_df_bigquery',
        python_callable = distinct_interest_rates_df_bigquery
    )

    cast_int_rate_df_bigquery = PythonOperator(
        task_id = 'cast_int_rate_df_bigquery',
        python_callable = cast_int_rate_df_bigquery
    )

    commodities_daily_df_bigquery = PythonOperator(
        task_id = 'commodities_daily_df_bigquery',
        python_callable = commodities_daily_df_bigquery
    )

    start_daily_transformation_postgres >> [create_stocks_daily_table, create_exchange_rates_daily_table, create_interest_rates_daily_table, create_table_gold_daily, create_table_silver_daily, create_table_crude_oil_daily, create_commodities_daily_table]
    
    create_stocks_daily_table >> create_final_stock_table >> insert_stocks_daily_table >> distinct_stocks_daily_table >> sma_stock_postgres >> stocks_daily_df_bigquery
    create_exchange_rates_daily_table  >> insert_exchange_rates_daily_table >> distinct_exchange_rates_daily_table >> exchange_rates_daily_df_bigquery
    create_interest_rates_daily_table >> create_final_interest_rate >> insert_interest_rates_daily_table >> distinct_interest_rates_daily_table >> lag_int_postgres >> cast_interest_rate_daily_table >> [distinct_interest_rates_df_bigquery, interest_rates_daily_df_bigquery, cast_int_rate_df_bigquery]
    create_table_gold_daily >> insert_gold_daily_table 
    create_table_silver_daily >> insert_silver_daily_table 
    create_table_crude_oil_daily >> insert_crude_oil_daily_table

    [insert_gold_daily_table, insert_silver_daily_table, insert_crude_oil_daily_table, create_commodities_daily_table] >> distinct_commodities_daily_table
    distinct_commodities_daily_table >> commodities_daily_df_bigquery
    
    [stocks_daily_df_bigquery, exchange_rates_daily_df_bigquery, interest_rates_daily_df_bigquery, distinct_interest_rates_df_bigquery, cast_int_rate_df_bigquery, commodities_daily_df_bigquery] >> end_daily_transformation_postgres

    return daily_postgres_taskgroup
