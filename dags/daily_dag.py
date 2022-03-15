from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.cloud import storage

import json
import os
import pandas as pd
import requests
import urllib.request
import yfinance as yf 

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = stockprediction_servicekey.json
storage_client = storage.Client()
bucket = client.get_bucket('stock_prediction_is3107')

# Add retry_delay later on when pipeline is up
default_args = {
    'owner': 'Nicole',
    'depends_on_past': False,
    'email': ['nicole@png.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # Remember to change this when the actual project kickstarts
    'start_date': datetime(2022, 3, 15)
}

dag = DAG(
    dag_id = 'daily_scrape',
    description = 'Scraping daily information on data related to prediction of stocks',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
)

# Load tickers that will be used
tickers_df = pd.read_csv('/home/airflow/airflow/dags/sti.csv')

############################
# Define Python Functions  #
############################

# Function to obtain yfinance historial prices
def get_stock_price(**kwargs):
    tickers_df = kwargs['df']
    sgx = list(tickers_df['New Symbol']) 
    stocks =[]
    # Loop to get all historical prices
    for ticker in sgx:
        print('Current Ticker is:', ticker)
        curr_ticker = yf.Ticker(ticker)
        curr_df = curr_ticker.history(start = kwargs['start'], end = kwargs['end'])

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
    print(df.head(1))
    return df.to_parquet('historical_prices.parquet', engine='fastparquet')

# Function to scrape interest rates
def interest_rate_scraping(**kwargs):
    # Reference to oldest date because 2018-01-01 was PH (initialisation)
    initial_date = kwargs['start']
    oldest_datetime_obj = datetime.strptime(initial_date, '%Y-%M-%d')
    
    # Obtain latest 1000 rows
    url = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=1000&sort=end_of_day%20desc'
    req = urllib.request.Request(url,None,headers)
    response = urllib.request.urlopen(req)
    data = response.read()
    raw_batch1 = json.loads(data.decode())
    batch1 = raw_batch1['result']['records']
    
    ir_init_batch = batch1.copy()
    print(len(ir_init_batch))
    
    # Check if first batch oldest date == 2018-01-02
    curr_old = batch1[999]['end_of_day']
    
    while(curr_old != '2018-01-02'):
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
        new_url = url + date_url
        curr_req = urllib.request.Request(new_url,None,headers)

        curr_response = urllib.request.urlopen(curr_req)
        curr_data = curr_response.read()

        curr_raw_batch = json.loads(curr_data.decode())
        curr_batch = curr_raw_batch['result']['records']
        
        ir_init_batch = ir_init_batch + curr_batch
        print(len(ir_init_batch))
        
        index = len(curr_batch)
        
        # Update condition:
        if curr_old == '2018-01-02':
            break
        else:
            curr_old = curr_batch[index-1]['end_of_day']

    df = pd.DataFrame(ir_init_batch)
    df = df[df['comp_sora_1m'].notna()]

    print("Interest Rates Obtained")
    return df.to_parquet('interest_rate.parquet', engine='fastparquet')

# Function to scrape exchange rate
def exchange_rate_scraping(**kwargs):
    # Reference to oldest date because 2018-01-01 was PH 
    # Reference to oldest date because 2018-01-01 was PH (initialisation)
    initial_date = kwargs['start']
    oldest_datetime_obj = datetime.strptime(initial_date, '%Y-%M-%d')
    
    # Obtain latest 1000 rows
    url = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=95932927-c8bc-4e7a-b484-68a66a24edfe&limit=1000&sort=end_of_day%20desc'
    req = urllib.request.Request(url,None,headers)
    response = urllib.request.urlopen(req)
    data = response.read()
    raw_batch1 = json.loads(data.decode())
    batch1 = raw_batch1['result']['records']
    
    er_init_batch = batch1.copy()
    print(len(er_init_batch))
    
    # Check if first batch oldest date == 2018-01-02
    curr_old = batch1[999]['end_of_day']
    
    while(curr_old != '2018-01-02'):
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
        new_url = url + date_url
        curr_req = urllib.request.Request(new_url,None,headers)

        curr_response = urllib.request.urlopen(curr_req)
        curr_data = curr_response.read()

        curr_raw_batch = json.loads(curr_data.decode())
        curr_batch = curr_raw_batch['result']['records']
        
        er_init_batch = er_init_batch + curr_batch
        print(len(er_init_batch))
        
        index = len(curr_batch)
        
        # Update condition:
        if curr_old == '2018-01-02':
            break
        else:
            curr_old = curr_batch[index-1]['end_of_day']


    print("Exchange Rate Obtained")
    df = pd.DataFrame(er_init_batch)

    return df.to_parquet('exchange_rate.parquet', engine='fastparquet')


# Choose between initialisation and daily path
def choose_path(**kwargs):
    # Connect to google cloud storage
    # Fetch to see if google cloud storage has the table already
    print("dummy")
    return None


def finish_init(**kwargs):
    print('Initialisation Done')


####################
# Define Operators #
####################

# Scraping Historical Stock Prices
ticker_scraping = PythonOperator(
    task_id = 'ticker_scraping_data',
    python_callable =  get_stock_price,
    # Need to fix op_kwargs to get the date as required
    op_kwargs = {'start':'2018-01-01', 'end':'2022-03-13', 'df': tickers_df},
    dag = dag
)

# Scraping Daily Exchange Rates
exchange_rate_scraping = PythonOperator(
    task_id = 'exchange_rate_scraping_data',
    python_callable = exchange_rate_scraping,
    op_kwargs = {'start': '2018-01-02'},
    dag = dag
)

# Scraping annual interest rates
interest_rate_scraping = PythonOperator(
    task_id = 'interest_rate_scraping_data',
    python_callable = interest_rate_scraping,
    op_kwargs = {'start': '2018-01-02'},
    dag = dag
)

# Start of DAG (to test)
start_init = BashOperator(
    task_id = 'start_task',
    bash_command = 'echo start',
    dag = dag
)

choose_best_path = BranchPythonOperator(
    task_id = 'choose_dag_path',
    python_callable = choose_path,
    dag = dag
)

finish_start_init = PythonOperator(
    task_id = 'finish_start_init',
    python_callable = finish_init,
    dag = dag
)

# Echo task finish (filler) (for testing dag paths)
finish_start = BashOperator(
    task_id = 'finish_task',
    bash_command = 'echo finish',
    dag = dag
)

############################
# Define Tasks Hierarchy   #
############################
start_init >> [ticker_scraping, exchange_rate_scraping, interest_rate_scraping] >> finish_start