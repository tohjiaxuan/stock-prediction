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
from pandas_datareader import data as pdr

import json
import os
import pandas as pd
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
DWH_DATASET = 'stock_prediction_datawarehouse'

curr_date = datetime.today().strftime('%Y-%m-%d')
yest_date = (datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')

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
    dag_id = 'daily_scrape_test',
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

'''# Check if files exists in GCS
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

# Function to scrape gold stock prices
def gold_scraping(**kwargs):
    start_date = kwargs['start']
    end_date = kwargs['end']

    gold = pdr.get_data_yahoo("GC=F", start=start_date, end=end_date)
    print('Gold Data Obtained')

    return gold

# Function to scrape silver stock prices
def silver_scraping(**kwargs):
    start_date = kwargs['start']
    end_date = kwargs['end']

    silver = pdr.get_data_yahoo("SI=F", start=start_date, end=end_date)
    print('Silver Data Obtained')

    return silver'''

# Function to scrape crude oil stock prices
def crude_oil_scraping(**kwargs):
    start_date = kwargs['start']
    end_date = kwargs['end']

    crude_oil = pdr.get_data_yahoo("CL=F", start=start_date, end=end_date)
    print('Crude Oil Data Obtained')

    return crude_oil

'''# Push stock data from XCOM to Cloud
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

# Push gold stock prices from XCOM to Clouds
def push_gold(**kwargs):
    curr_data = kwargs['task_instance'].xcom_pull(task_ids='gold_scraping_data')
    curr_data.to_parquet('gs://stock_prediction_is3107/gold.parquet')
    print("Pushing Gold Stock Prices to Cloud")

# Push silver stock prices from XCOM to Clouds
def push_silver(**kwargs):
    curr_data = kwargs['task_instance'].xcom_pull(task_ids='silver_scraping_data')
    curr_data.to_parquet('gs://stock_prediction_is3107/silver.parquet')
    print("Pushing Silver Stock Prices to Cloud")'''

# Push crude oil stock prices from XCOM to Clouds
def push_crude_oil(**kwargs):
    scraping_data = kwargs['task_instance'].xcom_pull(task_ids='crude_oil_scraping_data')
    updating_data = kwargs['task_instance'].xcom_pull(task_ids='crude_oil_updating_data')
    if isinstance(scraping_data, pd.DataFrame):
        scraping_data.to_parquet('gs://stock_prediction_is3107/crude_oil.parquet')
        print('Pushing Crude Oil Stock Prices to Cloud (Initialisation)')
    else:
        updating_data.to_parquet('gs://stock_prediction_is3107/crude_oil.parquet')
        print('Pushing Crude Oil Stock Prices to Cloud (Update)')
    '''curr_data = kwargs['task_instance'].xcom_pull(task_ids='crude_oil_scraping_data')
    curr_data.to_parquet('gs://stock_prediction_is3107/crude_oil.parquet')
    print("Pushing Crude Oil Stock Prices to Cloud")'''

def finish_init(**kwargs):
    print('Initialisation Done')


####################
# Define Operators #
####################
'''# Check if file exists in gcs (can change to dwh - better?)
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

# Scraping (initialise) gold stock prices
gold_scraping = PythonOperator(
    task_id = 'gold_scraping_data',
    python_callable = gold_scraping,
    op_kwargs = {'start': '2018-01-02', 'end': curr_date},
    dag = dag
)

# Scraping (initialise) silver stock prices
silver_scraping = PythonOperator(
    task_id = 'silver_scraping_data',
    python_callable = silver_scraping,
    op_kwargs = {'start': '2018-01-02', 'end': curr_date},
    dag = dag
)'''

# Scraping (initialise) crude oil stock prices
crude_oil_scraping = PythonOperator(
    task_id = 'crude_oil_scraping_data',
    python_callable = crude_oil_scraping,
    op_kwargs = {'start': '2018-01-02', 'end': curr_date},
    dag = dag
)

'''# Push Historical Stock Prices to Cloud
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

# Push Gold Stock Prices to Cloud
gold_cloud = PythonOperator(
    task_id = 'push_gold_cloud_data',
    python_callable = push_gold,
    dag = dag
)

# Push Silver Stock Prices to Cloud
silver_cloud = PythonOperator(
    task_id = 'push_silver_cloud_data',
    python_callable = push_silver,
    dag = dag
)'''

# Push Crude Oil Prices to Cloud
oil_cloud = PythonOperator(
    task_id = 'push_crude_oil_cloud_data',
    trigger_rule='none_failed_or_skipped',
    python_callable = push_crude_oil,
    dag = dag
)

###########
# Staging #
###########

'''# Load stock prices data from GCS to BQ
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

# Load gold stock prices data from GCS to BQ
load_gold = GoogleCloudStorageToBigQueryOperator(
    task_id = 'stage_gold',
    bucket = 'stock_prediction_is3107',
    source_objects = ['gold.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.init_gold',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load silver stock prices data from GCS to BQ
load_silver = GoogleCloudStorageToBigQueryOperator(
    task_id = 'stage_silver',
    bucket = 'stock_prediction_is3107',
    source_objects = ['silver.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.init_silver',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)'''

# Load crude oil stock prices data from GCS to BQ
load_crude_oil = GoogleCloudStorageToBigQueryOperator(
    task_id = 'stage_crude_oil',
    bucket = 'stock_prediction_is3107',
    source_objects = ['crude_oil.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.init_crude_oil',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Start of DAG (to test)
'''start_init = BashOperator(
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
)'''

########################################################
# TEST START                                           #
########################################################

# Function to update crude oil stock prices
def crude_oil_updating(**kwargs):
    today_date = kwargs['today']

    crude_oil = pdr.get_data_yahoo("CL=F", start=yest_date, end=today_date)
    print('Crude Oil Updated')

    return crude_oil

# Start of DAG (to test)
start_init = BashOperator(
    task_id = 'start_task',
    bash_command = 'echo start',
    dag = dag
)

# Echo task finish (filler) (for testing dag paths)
finish_start = BashOperator(
    task_id = 'finish_task',
    #trigger_rule='none_failed_or_skipped',
    bash_command = 'echo finish',
    dag = dag
)

# Start Initialiastion
init_crude_oil_db = BashOperator(
    task_id = 'init_crude_oil_db',
    bash_command = 'echo start',
    dag = dag
)

# Echo task finish (filler) (for testing dag paths)
run_crude_oil_daily = BashOperator(
    task_id = 'run_crude_oil_daily',
    bash_command = 'echo start',
    dag = dag
)

# Check if files exists in GCS
def check_crude_oil(**kwargs):
    crude_oil = kwargs['crude_oil']
    if (storage.Blob(bucket = bucket, name = crude_oil).exists(storage_client)):
        return ("Exists")
    else:
        return ("Initialise")

# Choose between initialisation and daily path
def choose_path(**kwargs):
    choose = kwargs['task_instance'].xcom_pull(task_ids='crude_oil_file_check')
    if choose == 'Exists':
        return 'run_crude_oil_daily'
    return 'init_crude_oil_db'

crude_oil_file_check = PythonOperator(
    task_id = 'crude_oil_file_check',
    python_callable = check_crude_oil,
    op_kwargs = {'crude_oil': 'crude_oil.parquet'},
    dag = dag
)

crude_oil_path = BranchPythonOperator(
    task_id = 'crude_oil_path',
    python_callable = choose_path,
    do_xcom_push = False,
    dag = dag
)

# Scraping (updating) crude oil stock prices
crude_oil_updating = PythonOperator(
    task_id = 'crude_oil_updating_data',
    python_callable = crude_oil_updating,
    op_kwargs = {'today': curr_date},
    dag = dag
)

########################################################
# TEST END                                             #
########################################################

############################
# Define Tasks Hierarchy   #
############################
'''start_init >> daily_file_check >> choose_best_path >> [start_daily, init_db]
init_db >> [ticker_scraping, interest_rate_scraping, exchange_rate_scraping, gold_scraping, silver_scraping, crude_oil_scraping]
ticker_scraping >> stock_cloud >> load_stock_prices
interest_rate_scraping >> interest_cloud >> load_interest_rates
exchange_rate_scraping >> exchange_cloud >> load_exchange_rates
gold_scraping >> gold_cloud >> load_gold
silver_scraping >> silver_cloud >> load_silver
crude_oil_scraping >> oil_cloud >> load_crude_oil
[load_stock_prices, load_interest_rates, load_exchange_rates, load_gold, load_silver, load_crude_oil] >> finish_start'''

##### TEST FOR CRUDE OIL
start_init >> crude_oil_file_check >> crude_oil_path >> [init_crude_oil_db, run_crude_oil_daily] 
init_crude_oil_db >> crude_oil_scraping >> oil_cloud 
run_crude_oil_daily >> crude_oil_updating >> oil_cloud 
oil_cloud >> load_crude_oil >> finish_start
