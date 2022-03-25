from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
from pandas_datareader import data as pdr
from google.cloud import bigquery
from airflow.utils.task_group import TaskGroup

import json
import os
import numpy as np
import pandas as pd
import pandas_ta as ta
import requests
import urllib.request
import yfinance as yf 


yf.pdr_override()

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'

def build_extract_taskgroup(dag: DAG) -> TaskGroup:
    extract_taskgroup = TaskGroup(group_id = 'extract_taskgroup')

    curr_date = datetime.today().strftime('%Y-%m-%d')


    # Load tickers that will be used
    tickers_df = pd.read_csv('/home/airflow/airflow/dags/sti.csv')

    ############################
    # Define Python Functions  #
    ############################
    def helper_retrieval(link, headers):
        req = urllib.request.Request(link, None, headers)
        response = urllib.request.urlopen(req)
        data = response.read()
        raw_data = json.loads(data.decode())
        batch = raw_data['result']['records']
        return batch

    ###############
    # Check Files #
    ###############

    # Check if hist stock exists in GCS
    def check_stock_prices(**kwargs):
        stock = kwargs['stock']
        if storage.Blob(bucket = bucket, name = stock).exists(storage_client):
            return ("Exists")
        return ("Initialise")

    def choose_stock_path(**kwargs):
        choose = kwargs['task_instance'].xcom_pull(task_ids='stock_file_check_task')
        if choose == 'Exists':
            return 'recent_date_task'
        return 'stock_scraping_data'

    # Check if exchange rate exists in GCS
    def check_ex_rate(**kwargs):
        ex_rate = kwargs['ex_rate']
        if storage.Blob(bucket = bucket, name = ex_rate).exists(storage_client):
            return ("Exists")
        return ("Initialise")

    def choose_ex_path(**kwargs):
        choose = kwargs['task_instance'].xcom_pull(task_ids='ex_file_check_task')
        if choose == 'Exists':
            return 'recent_date_task'
        return 'exchange_rate_scraping_data'

    # Check if interest rate exists in GCS
    def check_int_rate(**kwargs):
        int_rate = kwargs['int_rate']
        if storage.Blob(bucket = bucket, name = int_rate).exists(storage_client):
            return ("Exists")
        return ("Initialise")

    def choose_int_path(**kwargs):
        choose = kwargs['task_instance'].xcom_pull(task_ids='int_file_check_task')
        if choose == 'Exists':
            return 'recent_date_task'
        return 'interest_rate_scraping_data'

    # Check if gold exists in GCS
    def check_gold_prices(**kwargs):
        gold = kwargs['gold']
        if storage.Blob(bucket = bucket, name = gold).exists(storage_client):
            return ("Exists")
        return ("Initialise")

    def choose_gold_path(**kwargs):
        choose = kwargs['task_instance'].xcom_pull(task_ids='gold_file_check_task')
        if choose == 'Exists':
            return 'recent_date_task'
        return 'gold_scraping_data'

    # Check if silver exists in GCS
    def check_silver_prices(**kwargs):
        silver = kwargs['silver']
        if storage.Blob(bucket = bucket, name = silver).exists(storage_client):
            return ("Exists")
        return ("Initialise")

    def choose_silver_path(**kwargs):
        choose = kwargs['task_instance'].xcom_pull(task_ids='silver_file_check_task')
        if choose == 'Exists':
            return 'recent_date_task'
        return 'silver_scraping_data'

    # Check if crude oil exists in GCS
    def check_crude_oil(**kwargs):
        oil = kwargs['oil']
        if storage.Blob(bucket = bucket, name = oil).exists(storage_client):
            return ("Exists")
        return ("Initialise")

    def choose_crude_oil_path(**kwargs):
        choose = kwargs['task_instance'].xcom_pull(task_ids='crude_oil_file_check_task')
        if choose == 'Exists':
            return 'recent_date_task'
        return 'crude_oil_scraping_data'

    def get_recent_date(**kwargs):
        bq_client = bigquery.Client()
        query = "select MAX(`Date`) from `stockprediction-344203.stock_prediction_datawarehouse.F_STOCKS`"
        df = bq_client.query(query).to_dataframe()
        recent_date = df['f0_'].values[0]
        string_date = np.datetime_as_string(recent_date, unit='D')
        return string_date

    ###########################
    # Scraping Initialisation #
    ###########################
    # Function to obtain yfinance historial prices

    def helper_stock_price(tickers_df, start_date, end_date):
            
        sgx = list(tickers_df['New Symbol']) 
        stocks =[]

        # Loop to get all historical prices
        for ticker in sgx:
            print('Current Ticker is:', ticker)
            curr_ticker = yf.Ticker(ticker)
            curr_df = curr_ticker.history(start = start_date, end = end_date)

            # Check if df contains results
            if len(curr_df) == 0:
                print(ticker, " no information in timeframe specified")
                continue

            # Check for duplicated indices (timestamp) and remove them
            curr_df = curr_df.loc[~curr_df.index.duplicated(keep='last')]
            curr_df = curr_df.reset_index()
            curr_df['Stock'] = ticker
            stocks.append(curr_df)
        # Concatenate all dfs
        df = pd.concat(stocks)
        return df

    def initialise_stock_price(**kwargs):
        tickers_df = kwargs['df']
        start_date = kwargs['start']
        end_date = kwargs['end']
        stock_df = helper_stock_price(tickers_df, start_date, end_date)
        print("Obtained Initialisation Stock Prices")
        return stock_df

    def update_stock_price(**kwargs):
        tickers_df = kwargs['df']
        pulled_date = kwargs['task_instance'].xcom_pull(task_ids='recent_date_task')
        start_date = (datetime.strptime(pulled_date, '%Y-%M-%d') + timedelta(days=1)).strftime('%Y-%M-%d')
        end_date = kwargs['end']
        stock_df = helper_stock_price(tickers_df, start_date, end_date)
        print("Obtained Daily Stock Prices (Update)")
        return stock_df

    # Function to scrape exchange rate
    def helper_exchange_rate(initial_date):
        oldest_datetime_obj = datetime.strptime(initial_date, '%Y-%M-%d')
        
        # Obtain latest 1000 rows
        curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=95932927-c8bc-4e7a-b484-68a66a24edfe&limit=1000&sort=end_of_day%20desc'
        batch1 = helper_retrieval(curr_link, headers)
        
        er_init_batch = batch1.copy()
        
        # Check if new date is inside the 1000 records
        counter = 0
        for record in batch1:
            if record['end_of_day'] == initial_date:
                return pd.DataFrame(batch1[0:counter+1])
            else:
                counter += 1

        curr_old = batch1[999]['end_of_day']
        
        while(curr_old != initial_date):
            # Update new end_date and start date
            new_end = datetime.strptime(curr_old, '%Y-%M-%d') - timedelta(days=1)
            new_start = new_end - timedelta(days=1000)
            
            # If less than 1000 records or just nice 1000 days away, then can just use oldest
            if new_start <= oldest_datetime_obj:
                print("Less than 1000 days from initialisation date")
                date_url = '&between[end_of_day]=2018-01-01,'+ new_end.strftime('%Y-%M-%d')
                curr_old = '2018-01-02'
                
            else:
                print("More than 1000 days from initialisation date")
                date_url = '&between[end_of_day]=' + new_end.strftime('%Y-%M-%d') + ','+ new_end.strftime('%Y-%M-%d')
            
            # Get new requests
            new_url = curr_link + date_url
            curr_batch = helper_retrieval(new_url, headers)
            er_init_batch = er_init_batch + curr_batch
            
            index = len(curr_batch)
            
            # Update condition:
            if curr_old == initial_date:
                break
            else:
                curr_old = curr_batch[index-1]['end_of_day']
        
        df = pd.DataFrame(er_init_batch)

        return df

    def initialise_exchange_rate(**kwargs):
        start_date = kwargs['start']
        ex_df = helper_exchange_rate(start_date)
        print("Obtained Initialisation Exchange Rates")
        return ex_df

    def update_exchange_rate(**kwargs):
        pulled_date = kwargs['task_instance'].xcom_pull(task_ids='recent_date_task')
        start_date = (datetime.strptime(pulled_date, '%Y-%M-%d') + timedelta(days=1)).strftime('%Y-%M-%d')
        ex_df = helper_exchange_rate(start_date)
        print("Obtained Daily Exchange Rates (Update)")
        return ex_df

    # Function to scrape interest rate
    def helper_interest_rate(initial_date):
        oldest_datetime_obj = datetime.strptime(initial_date, '%Y-%M-%d')
        
        # Obtain latest 1000 rows
        curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=1000&sort=end_of_day%20desc'
        batch1 = helper_retrieval(curr_link, headers)
        
        ir_init_batch = batch1.copy()

        counter = 0
        for record in batch1:
            if record['end_of_day'] == initial_date:
                return pd.DataFrame(batch1[0:counter+1])
            else:
                counter += 1

        curr_old = batch1[999]['end_of_day']
        
        while(curr_old != initial_date):
            # Update new end_date and start date
            new_end = datetime.strptime(curr_old, '%Y-%M-%d') - timedelta(days=1)
            new_start = new_end - timedelta(days=1000)
            
            # If less than 1000 records or just nice 1000 days away, then can just use oldest
            if new_start <= oldest_datetime_obj:
                print("Less than 1000 days from initialisation date")
                date_url = '&between[end_of_day]=2018-01-01,'+ new_end.strftime('%Y-%M-%d')
                curr_old = '2018-01-02'
                
            else:
                print("More than 1000 days from initialisation date")
                date_url = '&between[end_of_day]=' + new_end.strftime('%Y-%M-%d') + ','+ new_end.strftime('%Y-%M-%d')
            
            # Get new requests
            new_url = curr_link + date_url
            curr_batch = helper_retrieval(new_url, headers)
            ir_init_batch = ir_init_batch + curr_batch
            
            index = len(curr_batch)
            
            # Update condition:
            if curr_old == initial_date:
                break
            else:
                curr_old = curr_batch[index-1]['end_of_day']

        df = pd.DataFrame(ir_init_batch)
        df = df[df['comp_sora_1m'].notna()]

        print("Interest Rates Obtained")
        return df
    
    def initialise_interest_rate(**kwargs):
        start_date = kwargs['start']
        int_df = helper_interest_rate(start_date)
        print("Obtained Initialisation Interest Rates")
        return int_df
    
    def update_interest_rate(**kwargs):
        pulled_date = kwargs['task_instance'].xcom_pull(task_ids='recent_date_task')
        start_date = (datetime.strptime(pulled_date, '%Y-%M-%d') + timedelta(days=1)).strftime('%Y-%M-%d')
        int_df = helper_interest_rate(start_date)
        print("Obtained Daily Interest Rates (Update)")
        return int_df

    # Push stock data from XCOM to Cloud
    def push_stock_price(**kwargs):
        scraping_data = kwargs['ti'].xcom_pull(task_ids='stock_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_stock_scraping_data')
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/stock_prices.parquet')
            print("Pushing Historical Stock Prices to Cloud")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/stock_prices.parquet')
            print("Pushing New Stock Prices to Cloud")

    # Push e/r data from XCOM to Clouds
    def push_exchange_rate(**kwargs):
        scraping_data = kwargs['ti'].xcom_pull(task_ids='exchange_rate_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_exchange_rate_scraping_data')
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/exchange_rate.parquet')
            print("Pushing Initialisation Exchange Rates to Cloud")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/exchange_rate.parquet')
            print("Pushing New Exchange Rates to Cloud")
    
    # Push i/r data from XCOM to Clouds
    def push_interest_rate(**kwargs):
        scraping_data = kwargs['ti'].xcom_pull(task_ids='interest_rate_scraping_data')
        updating_data = kwargs['ti'].xcom_pull(task_ids='update_interest_rate_scraping_data')
        if isinstance(scraping_data, pd.DataFrame):
            scraping_data.to_parquet('gs://stock_prediction_is3107/interest_rate.parquet')
            print("Pushing Initialisation Interest Rates to Cloud")
        else:
            updating_data.to_parquet('gs://stock_prediction_is3107/interest_rate.parquet')
            print("Pushing New Interest Rates to Cloud")

    ####################
    # Define Operators #
    ####################
    ###############
    # Check Paths #
    ###############
    # Check if file exists in gcs (can change to dwh - better?)
    stock_file_check = PythonOperator(
        task_id = 'stock_file_check_task',
        python_callable = check_stock_prices,
        op_kwargs = {'stock': 'stock_prices.parquet'},
        dag = dag
    )

    stock_path = BranchPythonOperator(
        task_id = 'stock_path_task',
        python_callable = choose_stock_path,
        do_xcom_push = False,
        dag = dag
    )

    ex_file_check = PythonOperator(
        task_id = 'ex_file_check_task',
        python_callable = check_ex_rate,
        op_kwargs = {'ex_rate': 'exchange_rate.parquet'},
        dag = dag
    )

    ex_path = BranchPythonOperator(
        task_id = 'ex_path_task',
        python_callable = choose_ex_path,
        do_xcom_push = False,
        dag = dag
    )

    int_file_check = PythonOperator(
        task_id = 'int_file_check_task',
        python_callable = check_int_rate,
        op_kwargs = {'int_rate': 'interest_rate.parquet'},
        dag = dag
    )

    int_path = BranchPythonOperator(
        task_id = 'int_path_task',
        python_callable = choose_int_path,
        do_xcom_push = False,
        dag = dag
    )

    ##################################
    # Extract Stage (Initialisation) #
    ##################################

    # Scraping Initialise Historical Stock Prices
    stock_scraping = PythonOperator(
        task_id = 'stock_scraping_data',
        python_callable = initialise_stock_price,
        op_kwargs = {'start':'2018-01-01', 'end': curr_date, 'df': tickers_df},
        provide_context = True,
        dag = dag
    )

    # Scraping Initialise Daily Exchange Rates
    exchange_rate_scraping = PythonOperator(
        task_id = 'exchange_rate_scraping_data',
        python_callable = initialise_exchange_rate,
        op_kwargs = {'start': '2018-01-02'},
        provide_context = True,
        dag = dag
    )

    # Scraping Initialise Daily Interest Rates
    interest_rate_scraping = PythonOperator(
        task_id = 'interest_rate_scraping_data',
        python_callable = initialise_interest_rate,
        op_kwargs = {'start': '2018-01-02'},
        provide_context = True,
        dag = dag
    )

    ############################
    # Extract Stage (Updating) #
    ############################
    update_stock_scraping = PythonOperator(
        task_id = 'update_stock_scraping_data',
        python_callable = update_stock_price,
        op_kwargs = {'end': curr_date, 'df': tickers_df},
        provide_context = True,
        dag = dag
    )

    update_exchange_rate_scraping = PythonOperator(
        task_id = 'update_exchange_rate_scraping_data',
        python_callable = update_exchange_rate,
        op_kwargs = {'end': curr_date, 'df': tickers_df},
        provide_context = True,
        dag = dag
    )

    update_interest_rate_scraping = PythonOperator(
        task_id = 'update_interest_rate_scraping_data',
        python_callable = update_interest_rate,
        op_kwargs = {'end': curr_date, 'df': tickers_df},
        provide_context = True,
        dag = dag
    )    

    # Start of DAG (to test)
    start_init = BashOperator(
        task_id = 'start_task',
        bash_command = 'echo start',
        dag = dag
    )

    recent_date = PythonOperator(
        task_id = 'recent_date_task',
        python_callable = get_recent_date,
        trigger_rule = 'none_failed',
        provide_context = True,
        dag = dag
    )

    ####################
    # Pushing to Cloud #
    ####################

    # Push Historical Stock Prices to Cloud
    stock_cloud = PythonOperator(
        task_id = 'push_stock_cloud_data',
        trigger_rule = 'none_failed',
        python_callable = push_stock_price,
        provide_context = True,
        dag = dag
    )

    # Push Exchange Rates to Cloud
    exchange_cloud = PythonOperator(
        task_id = 'push_exchange_cloud_data',
        trigger_rule = 'none_failed',
        python_callable = push_exchange_rate,
        provide_context = True,
        dag = dag
    )

    # Push Interest Rates to Cloud
    interest_cloud = PythonOperator(
        task_id = 'push_interest_cloud_data',
        trigger_rule = 'none_failed',
        python_callable = push_interest_rate,
        provide_context = True,
        dag = dag
    )

    start_init >> [stock_file_check, ex_file_check, int_file_check]
    stock_file_check >> stock_path >> [stock_scraping, recent_date] 
    ex_file_check >> ex_path >> [exchange_rate_scraping, recent_date]
    int_file_check >> int_path >> [interest_rate_scraping, recent_date]


    recent_date >> [update_stock_scraping, update_exchange_rate_scraping, update_interest_rate_scraping]
    [stock_scraping, update_stock_scraping] >> stock_cloud
    [exchange_rate_scraping, update_exchange_rate_scraping] >> exchange_cloud
    [interest_rate_scraping, update_interest_rate_scraping] >> interest_cloud

    return extract_taskgroup