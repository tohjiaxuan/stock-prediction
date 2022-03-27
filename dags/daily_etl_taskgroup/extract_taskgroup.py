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
    
    def helper_latest_retrieval(website_link):
        latest_retrival = helper_retrieval(website_link, headers)
        latest_mas_date = latest_retrival[0]['end_of_day']
        latest_mas_date = datetime.strptime(latest_mas_date, '%Y-%M-%d')
        return latest_mas_date

    #############
    # Check DWH #
    #############
    
    # Check DWH for stock prices related items (stock prices, exchange rate, interest rate)
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

    # Check DWH for commodities related items (gold, silver, crude oil)
    def if_d_commodities_exists():
        try:
            metadata = bq_client.dataset(DWH_DATASET)
            table_ref = metadata.table('D_COMMODITIES')
            bq_client.get_table(table_ref)
            return True
        except:
            return False

    def get_recent_commodities_date(): # to edit
        bq_client = bigquery.Client()
        query = "select MAX(`Date`) from `stockprediction-344203.stock_prediction_datawarehouse.D_COMMODITIES`"
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
                date_url = '&between[end_of_day]=' + new_start.strftime('%Y-%M-%d') + ','+ new_end.strftime('%Y-%M-%d')
            
            # Get new requests
            new_url = curr_link + date_url
            curr_batch = helper_retrieval(new_url, headers)
            index = len(curr_batch)
            if index == 0:
                break
            er_init_batch = er_init_batch + curr_batch
            
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
        # Check if the most recent date in the data source is >= to the new start_date
        curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=95932927-c8bc-4e7a-b484-68a66a24edfe&limit=1&sort=end_of_day%20desc'
        latest_mas_date = helper_latest_retrieval(curr_link)
        if latest_mas_date >= datetime.strptime(start_date, '%Y-%M-%d'):
            ex_df = helper_exchange_rate(start_date)
            print("Obtained Daily Exchange Rates (Update)")

        else:
            # Create empty dataframe
            col_names = ['end_of_day', 'eur_sgd', 'gbp_sgd', 'usd_sgd', 'aud_sgd', 'cad_sgd', 'cny_sgd_100',
            'hkd_sgd_100', 'inr_sgd_100', 'idr_sgd_100', 'jpy_sgd_100', 'krw_sgd_100', 'myr_sgd_100',
            'twd_sgd_100', 'nzd_sgd', 'php_sgd_100', 'qar_sgd_100', 'sar_sgd_100', 'chf_sgd', 'thb_sgd_100',
            'aed_sgd_100', 'vnd_sgd_100']
            ex_df = pd.DataFrame(columns=col_names)
            print("No new exchange rate data to collect")
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
                date_url = '&between[end_of_day]=' + new_start.strftime('%Y-%M-%d') + ','+ new_end.strftime('%Y-%M-%d')
            
            # Get new requests
            new_url = curr_link + date_url
            curr_batch = helper_retrieval(new_url, headers)
            index = len(curr_batch)
            if index == 0:
                break
            ir_init_batch = ir_init_batch + curr_batch
            
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
        curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=1&sort=end_of_day%20desc'
        latest_mas_date = helper_latest_retrieval(curr_link)
        if latest_mas_date >= datetime.strptime(start_date, "%Y-%M-%d"):
            int_df = helper_interest_rate(start_date)
            print("Obtained Daily Interest Rates (Update)")
        else:
            # Create empty dataframe
            col_names = ['end_of_day', 'interbank_overnight', 'interbank_1w', 'interbank_1m', 'interbank_2m', ' interbank_3m',
            'interbank_6m', 'interbank_12m', 'commercial_bills_3m', 'usd_sibor_3m', 'sgs_repo_overnight_rate', 'standing_facility_deposit',
            'rmb_overnight_rate', 'sora', 'sora_index', 'comp_sora_1m', 'comp_sora_3m', 'comp_sora_6m',
            'aggregate_volume', 'highest_transaction', 'lowest_transaction', 'calculation_method']
            int_df = pd.DataFrame(columns=col_names)
        return int_df

    def pull_older_interest_rate(pulled_date):
        curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=1000&sort=end_of_day%20desc'
        new_start = datetime.strptime(pulled_date, '%Y-%M-%d') - timedelta(days=7)
        new_end = datetime.strptime(pulled_date, '%Y-%M-%d') - timedelta(days=1)
        date_url = '&between[end_of_day]=' + new_start.strftime('%Y-%M-%d') + ','+ new_end.strftime('%Y-%M-%d')
        new_url = curr_link + date_url
        new_batch = helper_retrieval(new_url, headers)
        df = pd.DataFrame(new_batch)
        df = df.head(1)
        return df
        
    def interest_rate():
        check_dwh = if_f_stock_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            t_lag_df = pull_older_interest_rate(pulled_date)
            t_df = update_interest_rate(pulled_date)
        else:
            t_lag_df = pull_older_interest_rate('2018-01-02')
            t_df = initialise_interest_rate('2018-01-02')
        int_df = t_df.append(t_lag_df, ignore_index = True)

        return int_df

    # Function to scrape gold prices
    def initialise_gold_prices(start_date, end_date):
        gold_df = pdr.get_data_yahoo("GC=F", start=start_date, end=end_date)
        gold_df = gold_df.reset_index() # so 'Date' becomes a column
        print("Obtained Gold Prices")
        return gold_df

    def update_gold_prices(pulled_date):
        yest_date = (datetime.strptime(pulled_date, '%Y-%M-%d') - timedelta(days=1)).strftime('%Y-%M-%d')
        gold_df = pdr.get_data_yahoo("GC=F", start=yest_date, end=pulled_date)
        gold_df = gold_df.reset_index() # so 'Date' becomes a column
        print("Obtained Daily Gold Prices (Update)")
        return gold_df

    def gold_prices():
        check_dwh = if_d_commodities_exists()
        if check_dwh:
            pulled_date = get_recent_commodities_date()
            gold_df = update_gold_prices(pulled_date)
        else:
            gold_df = initialise_gold_prices('2018-01-02', curr_date)
        return gold_df

    # Function to scrape silver prices
    def initialise_silver_prices(start_date, end_date):
        silver_df = pdr.get_data_yahoo("SI=F", start=start_date, end=end_date)
        silver_df = silver_df.reset_index() # so 'Date' becomes a column
        print("Obtained Silver Prices")
        return silver_df

    def update_silver_prices(pulled_date):
        yest_date = (datetime.strptime(pulled_date, '%Y-%M-%d') - timedelta(days=1)).strftime('%Y-%M-%d')
        silver_df = pdr.get_data_yahoo("SI=F", start=yest_date, end=pulled_date)
        silver_df = silver_df.reset_index() # so 'Date' becomes a column
        print("Obtained Daily Silver Prices (Update)")
        return silver_df

    def silver_prices():
        check_dwh = if_d_commodities_exists()
        if check_dwh:
            pulled_date = get_recent_commodities_date()
            silver_df = update_silver_prices(pulled_date)
        else:
            silver_df = initialise_silver_prices('2018-01-02', curr_date)
        return silver_df

    # Function to scrape crude oil prices
    def initialise_crude_oil_prices(start_date, end_date):
        crude_oil_df = pdr.get_data_yahoo("CL=F", start=start_date, end=end_date)
        crude_oil_df = crude_oil_df.reset_index() # so 'Date' becomes a column
        print("Obtained Crude Oil Prices")
        return crude_oil_df

    def update_crude_oil_prices(pulled_date):
        yest_date = (datetime.strptime(pulled_date, '%Y-%M-%d') - timedelta(days=1)).strftime('%Y-%M-%d')
        crude_oil_df = pdr.get_data_yahoo("CL=F", start=yest_date, end=pulled_date)
        crude_oil_df = crude_oil_df.reset_index() # so 'Date' becomes a column
        print("Obtained Daily Crude Oil Prices (Update)")
        return crude_oil_df

    def crude_oil_prices():
        check_dwh = if_d_commodities_exists()
        if check_dwh:
            pulled_date = get_recent_commodities_date()
            crude_oil_df = update_crude_oil_prices(pulled_date)
        else:
            crude_oil_df = initialise_crude_oil_prices('2018-01-02', curr_date)
        return crude_oil_df

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

    # Scraping Daily Gold Prices
    gold_scraping = PythonOperator(
        task_id = 'gold_scraping_data',
        python_callable = gold_prices,
        dag = dag
    )

    # Scraping Daily Silver Prices
    silver_scraping = PythonOperator(
        task_id = 'silver_scraping_data',
        python_callable = silver_prices,
        dag = dag
    )

    # Scraping Daily Crude Oil Prices
    crude_oil_scraping = PythonOperator(
        task_id = 'crude_oil_scraping_data',
        python_callable = crude_oil_prices,
        dag = dag
    )

    # Start of DAG (to test)
    start_init = BashOperator(
        task_id = 'start_task',
        bash_command = 'echo start',
        dag = dag
    )

    start_init >> [stock_scraping, exchange_rate_scraping, interest_rate_scraping, gold_scraping, silver_scraping, crude_oil_scraping]

    return extract_taskgroup