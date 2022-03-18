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

import json
import os
import pandas as pd
import requests
import urllib.request
import yfinance as yf 

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'

curr_date = datetime.today().strftime('%Y-%m-%d')

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

# Execute Query with Hook
def execute_query_with_hook(query):
    hook = PostgresHook(postgres_conn_id="postgres_local")
    hook.run(query)

# Check if files exists in GCS
def check_files(**kwargs):
    stock = kwargs['stock']
    int_rate = kwargs['int_rate']
    ex_rate = kwargs['ex_rate']

    if ((storage.Blob(bucket = bucket, name = stock).exists(storage_client)) and
    (storage.Blob(bucket=bucket, name=int_rate).exists(storage_client)) and 
    (storage.Blob(bucket=bucket, name=ex_rate).exists(storage_client))):
        return ("Exists")
    else:
        return ("Initialise")

# Choose between initialisation and daily path
def choose_path(**kwargs):
    choose = kwargs['task_instance'].xcom_pull(task_ids='daily_file_check_task')
    if choose == 'Exists':
        return 'start_daily_task'
    return 'init_db_task'

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
    return df

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
    
    # Check if first batch oldest date == 2018-01-02
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
        new_url = url + date_url
        curr_req = urllib.request.Request(new_url,None,headers)

        curr_response = urllib.request.urlopen(curr_req)
        curr_data = curr_response.read()

        curr_raw_batch = json.loads(curr_data.decode())
        curr_batch = curr_raw_batch['result']['records']
        
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
    
    # Check if first batch oldest date == 2018-01-02
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
        new_url = url + date_url
        curr_req = urllib.request.Request(new_url,None,headers)

        curr_response = urllib.request.urlopen(curr_req)
        curr_data = curr_response.read()

        curr_raw_batch = json.loads(curr_data.decode())
        curr_batch = curr_raw_batch['result']['records']
        
        er_init_batch = er_init_batch + curr_batch
        
        index = len(curr_batch)
        
        # Update condition:
        if curr_old == initial_date:
            break
        else:
            curr_old = curr_batch[index-1]['end_of_day']

    print("Exchange Rate Obtained")
    df = pd.DataFrame(er_init_batch)

    return df

# Push stock data from XCOM to Cloud
def push_stock_price(**kwargs):
    curr_data = kwargs['task_instance'].xcom_pull(task_ids='ticker_scraping_data')
    curr_data.to_parquet('gs://stock_prediction_is3107/stock_prices.parquet')
    print("Pushing Historical Stock Prices to Cloud")

# Push i/r data from XCOM to Clouds
def push_interest_rate(**kwargs):
    curr_data = kwargs['task_instance'].xcom_pull(task_ids='interest_rate_scraping_data')
    curr_data.to_parquet('gs://stock_prediction_is3107/interest_rate.parquet')
    print("Pushing Interest Rates to Cloud")

# Push e/r data from XCOM to Clouds
def push_exchange_rate(**kwargs):
    curr_data = kwargs['task_instance'].xcom_pull(task_ids='exchange_rate_scraping_data')
    curr_data.to_parquet('gs://stock_prediction_is3107/exchange_rate.parquet')
    print("Pushing Exchange Rates to Cloud")

def finish_init(**kwargs):
    print('Initialisation Done')


####################
# Define Operators #
####################
# Check if file exists in gcs (can change to dwh - better?)
daily_file_check = PythonOperator(
    task_id = 'daily_file_check_task',
    python_callable = check_files,
    op_kwargs = {'stock': 'stock_prices.parquet', 
    'int_rate': 'interest_rate.parquet', 
    'ex_rate': 'exchange_rate.parquet'},
    dag = dag
)

choose_best_path = BranchPythonOperator(
    task_id = 'choose_best_path_task',
    python_callable = choose_path,
    do_xcom_push = False,
    dag = dag
)
##################
# Extract Stage #
##################

# Scraping Initialise Historical Stock Prices
ticker_scraping = PythonOperator(
    task_id = 'ticker_scraping_data',
    python_callable = get_stock_price,
    # Need to fix op_kwargs to get the date as required
    op_kwargs = {'start':'2018-01-01', 'end': curr_date, 'df': tickers_df},
    dag = dag
)

# Scraping (initialise) Daily Exchange Rates
exchange_rate_scraping = PythonOperator(
    task_id = 'exchange_rate_scraping_data',
    python_callable = exchange_rate_scraping,
    op_kwargs = {'start': '2018-01-02'},
    dag = dag
)

# Scraping (initialise) annual interest rates
interest_rate_scraping = PythonOperator(
    task_id = 'interest_rate_scraping_data',
    python_callable = interest_rate_scraping,
    op_kwargs = {'start': '2018-01-02'},
    dag = dag
)

# Push Historical Stock Prices to Cloud
stock_cloud = PythonOperator(
    task_id = 'push_stock_cloud_data',
    python_callable = push_stock_price,
    dag = dag
)

# Push Interest Rates to Cloud
interest_cloud = PythonOperator(
    task_id = 'push_interest_cloud_data',
    python_callable = push_interest_rate,
    dag = dag
)

# Push Exchange Rates to Cloud
exchange_cloud = PythonOperator(
    task_id = 'push_exchange_cloud_data',
    python_callable = push_exchange_rate,
    dag = dag
)

###########
# Staging #
###########

# Load stock prices data from GCS to BQ
load_stock_prices = GoogleCloudStorageToBigQueryOperator(
    task_id = 'stage_stock_prices',
    bucket = 'stock_prediction_is3107',
    source_objects = ['stock_prices.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.init_hist_stock_prices',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load interest rate data from GCS to BQ
load_interest_rates = GoogleCloudStorageToBigQueryOperator(
    task_id = 'stage_interest_rate',
    bucket = 'stock_prediction_is3107',
    source_objects = ['interest_rate.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.init_interest_rates',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load exchange rate data from GCS to BQ
load_exchange_rates = GoogleCloudStorageToBigQueryOperator(
    task_id = 'stage_exchange_rate',
    bucket = 'stock_prediction_is3107',
    source_objects = ['exchange_rate.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.init_exchange_rates',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Start of DAG (to test)
start_init = BashOperator(
    task_id = 'start_task',
    bash_command = 'echo start',
    dag = dag
)

# Start Initialiastion
init_db = BashOperator(
    task_id = 'init_db_task',
    bash_command = 'echo start',
    dag = dag
)

# Echo task finish (filler) (for testing dag paths)
finish_start = BashOperator(
    task_id = 'finish_task',
    bash_command = 'echo finish',
    dag = dag
)

# Echo task finish (filler) (for testing dag paths)
start_daily = BashOperator(
    task_id = 'start_daily_task',
    bash_command = 'echo start',
    dag = dag
)

############################
# Define Tasks Hierarchy   #
############################
start_init >> daily_file_check >> choose_best_path >> [start_daily, init_db]
init_db >> [ticker_scraping, interest_rate_scraping, exchange_rate_scraping] 
ticker_scraping >> stock_cloud >> load_stock_prices
interest_rate_scraping >> interest_cloud >> load_interest_rates
exchange_rate_scraping >> exchange_cloud >> load_exchange_rates

[load_stock_prices, load_interest_rates, load_exchange_rates] >> finish_start