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
bq_client = bigquery.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'

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

    #############
    # Check DWH #
    #############
    
    def if_f_stock_exists():
        try:
            metadata = bq_client.dataset(DWH_DATASET)
            table_ref = metadata.table('F_STOCKS')
            bq_client.get_table(table_ref)
            return True
        except:
            return False

    def get_recent_date():
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

    def helper_stock_price(tickers, start_date, end_date):
            
        sgx = list(tickers['New Symbol']) 
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

    def initialise_stock_price(start_date, end_date):
        stock_df = helper_stock_price(tickers_df, start_date, end_date)
        print("Obtained Initialisation Stock Prices")
        return stock_df

    def update_stock_price(pulled_date, end_date):
        start_date = (datetime.strptime(pulled_date, '%Y-%M-%d') + timedelta(days=1)).strftime('%Y-%M-%d')
        stock_df = helper_stock_price(tickers_df, start_date, end_date)
        print("Obtained Daily Stock Prices (Update)")
        return stock_df
    
    def stock_prices():
        check_dwh = if_f_stock_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            stock_df = update_stock_price(pulled_date, curr_date)
        else:
            stock_df = initialise_stock_price('2018-01-01', curr_date)
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

    def initialise_exchange_rate(start_date):
        ex_df = helper_exchange_rate(start_date)
        print("Obtained Initialisation Exchange Rates")
        return ex_df

    def update_exchange_rate(pulled_date):
        start_date = (datetime.strptime(pulled_date, '%Y-%M-%d') + timedelta(days=1)).strftime('%Y-%M-%d')
        ex_df = helper_exchange_rate(start_date)
        print("Obtained Daily Exchange Rates (Update)")
        return ex_df

    def exchange_rate():
        check_dwh = if_f_stock_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            ex_df = update_exchange_rate(pulled_date)
        else:
            ex_df = initialise_exchange_rate('2018-01-02')
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
    
    def initialise_interest_rate(start_date):
        int_df = helper_interest_rate(start_date)
        print("Obtained Initialisation Interest Rates")
        return int_df
    
    def update_interest_rate(pulled_date):
        start_date = (datetime.strptime(pulled_date, '%Y-%M-%d') + timedelta(days=1)).strftime('%Y-%M-%d')
        int_df = helper_interest_rate(start_date)
        print("Obtained Daily Interest Rates (Update)")
        return int_df

    def interest_rate():
        check_dwh = if_f_stock_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            int_df = update_interest_rate(pulled_date)
        else:
            int_df = initialise_interest_rate('2018-01-02')
        return int_df

    ####################
    # Define Operators #
    ####################

    #################
    # Extract Stage #
    #################

    # Scraping Historical Stock Prices
    stock_scraping = PythonOperator(
        task_id = 'stock_scraping_data',
        python_callable = stock_prices,
        dag = dag
    )

    # Scraping Daily Exchange Rates
    exchange_rate_scraping = PythonOperator(
        task_id = 'exchange_rate_scraping_data',
        python_callable = exchange_rate,
        dag = dag
    )

    # Scraping Daily Interest Rates
    interest_rate_scraping = PythonOperator(
        task_id = 'interest_rate_scraping_data',
        python_callable = interest_rate,
        dag = dag
    )

    # Start of DAG (to test)
    start_init = BashOperator(
        task_id = 'start_task',
        bash_command = 'echo start',
        dag = dag
    )

    start_init >> [stock_scraping, exchange_rate_scraping, interest_rate_scraping]

    return extract_taskgroup