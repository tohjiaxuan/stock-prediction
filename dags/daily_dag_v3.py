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

def helper_date(task, initial):
    pulled_date = task.xcom_pull(task_ids='recent_date_task')
    if isinstance(pulled_date, str):
        return_date = (datetime.strptime(pulled_date, '%Y-%M-%d') + timedelta(days=1)).strftime('%Y-%M-%d')
    else:
        return_date = initial
    return return_date

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
        return 'run_stock_daily_task'
    return 'ticker_scraping_data'

# Check if exchange rate exists in GCS
def check_ex_rate(**kwargs):
    ex_rate = kwargs['ex_rate']
    if storage.Blob(bucket = bucket, name = ex_rate).exists(storage_client):
        return ("Exists")
    return ("Initialise")

def choose_ex_path(**kwargs):
    choose = kwargs['task_instance'].xcom_pull(task_ids='ex_file_check_task')
    if choose == 'Exists':
        return 'run_ex_daily_task'
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
        return 'run_int_daily_task'
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
        return 'run_gold_daily_task'
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
        return 'run_silver_daily_task'
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
        return 'run_crude_oil_daily_task'
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
def get_stock_price(**kwargs):
    tickers_df = kwargs['df']
    
    begin_date = helper_date(kwargs['task_instance'], kwargs['start'])
    
    sgx = list(tickers_df['New Symbol']) 
    stocks =[]
    # Loop to get all historical prices
    for ticker in sgx:
        print('Current Ticker is:', ticker)
        curr_ticker = yf.Ticker(ticker)
        curr_df = curr_ticker.history(start = begin_date, end = kwargs['end'])

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

# Function to scrape exchange rate
def exchange_rate_scraping(**kwargs):
    # Reference to oldest date because 2018-01-01 was PH 
    # Reference to oldest date because 2018-01-01 was PH (initialisation)
    initial_date = kwargs['start']
    oldest_datetime_obj = datetime.strptime(initial_date, '%Y-%M-%d')
    
    # Obtain latest 1000 rows
    curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=95932927-c8bc-4e7a-b484-68a66a24edfe&limit=1000&sort=end_of_day%20desc'
    batch1 = helper_retrieval(curr_link, headers)
    
    er_init_batch = batch1.copy()
    
    # Check if new date is inside the 1000 records
    begin_date = helper_date(kwargs['task_instance'], initial_date)

    counter = 0
    for record in batch1:
        if record['end_of_day'] == begin_date:
            return pd.DataFrame(batch1[0:counter+1])
        else:
            counter += 1

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
        curr_batch = helper_retrieval(new_url, headers)
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

# Function to scrape interest rates
def interest_rate_scraping(**kwargs):
    # Reference to oldest date because 2018-01-01 was PH (initialisation)
    initial_date = kwargs['start']
    oldest_datetime_obj = datetime.strptime(initial_date, '%Y-%M-%d')
    
    # Obtain latest 1000 rows
    curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=1000&sort=end_of_day%20desc'
    batch1 = helper_retrieval(curr_link, headers)
    
    ir_init_batch = batch1.copy()
    # Check if new date is inside the 1000 records
    begin_date = helper_date(kwargs['task_instance'], initial_date)

    counter = 0
    for record in batch1:
        if record['end_of_day'] == begin_date:
            return pd.DataFrame(batch1[0:counter+1])
        else:
            counter += 1

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

    return silver

# Function to scrape crude oil stock prices
def crude_oil_scraping(**kwargs):
    start_date = kwargs['start']
    end_date = kwargs['end']

    crude_oil = pdr.get_data_yahoo("CL=F", start=start_date, end=end_date)
    print('Crude Oil Data Obtained')

    return crude_oil

####################
# Pushing to Cloud #
####################

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

# Push gold stock prices from XCOM to Clouds
def push_gold(**kwargs):
    curr_data = kwargs['task_instance'].xcom_pull(task_ids='gold_scraping_data')
    curr_data.to_parquet('gs://stock_prediction_is3107/gold.parquet')
    print("Pushing Gold Stock Prices to Cloud")

# Push silver stock prices from XCOM to Clouds
def push_silver(**kwargs):
    curr_data = kwargs['task_instance'].xcom_pull(task_ids='silver_scraping_data')
    curr_data.to_parquet('gs://stock_prediction_is3107/silver.parquet')
    print("Pushing Silver Stock Prices to Cloud")

# Push crude oil stock prices from XCOM to Clouds
def push_crude_oil(**kwargs):
    curr_data = kwargs['task_instance'].xcom_pull(task_ids='crude_oil_scraping_data')
    curr_data.to_parquet('gs://stock_prediction_is3107/crude_oil.parquet')
    print("Pushing Crude Oil Stock Prices to Cloud")

###################
# Transformation  #
###################
# Todo: need to edit to take in the normal one... daily
def sma_prices(**kwargs):
    bq_client = bigquery.Client()
    query = "select * from `stockprediction-344203.stock_prediction_staging_dataset.distinct_hist_stock_prices`"
    df = bq_client.query(query).to_dataframe()

    # Check for null values, if present, do ffill
    if (df.isnull().values.any() == True):
        df = df.fillna(method='ffill')

    # Calculate GC
    df["GC"] = df.ta.sma(50, append=True) > df.ta.sma(200, append=True)

    # Calculate DC
    df["DC"] = df.ta.sma(50, append=True) < df.ta.sma(200, append=True)

    print("Transforming Stocks Data Complete")
    df.to_parquet('gs://stock_prediction_is3107/final_stock.parquet')
    return df

def finish_init(**kwargs):
    print('Initialisation Done')

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

gold_file_check = PythonOperator(
    task_id = 'gold_file_check_task',
    python_callable = check_gold_prices,
    op_kwargs = {'gold': 'gold.parquet'},
    dag = dag
)

gold_path = BranchPythonOperator(
    task_id = 'gold_path_task',
    python_callable = choose_gold_path,
    do_xcom_push = False,
    dag = dag
)

silver_file_check = PythonOperator(
    task_id = 'silver_file_check_task',
    python_callable = check_silver_prices,
    op_kwargs = {'silver': 'silver.parquet'},
    dag = dag
)

silver_path = BranchPythonOperator(
    task_id = 'silver_path_task',
    python_callable = choose_silver_path,
    do_xcom_push = False,
    dag = dag
)

crude_oil_file_check = PythonOperator(
    task_id = 'crude_oil_file_check_task',
    python_callable = check_crude_oil,
    op_kwargs = {'oil': 'crude_oil.parquet'},
    dag = dag
)

crude_oil_path = BranchPythonOperator(
    task_id = 'crude_oil_path_task',
    python_callable = choose_crude_oil_path,
    do_xcom_push = False,
    dag = dag
)

##################################
# Extract Stage (Initialisation) #
##################################

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
)

# Scraping (initialise) crude oil stock prices
crude_oil_scraping = PythonOperator(
    task_id = 'crude_oil_scraping_data',
    python_callable = crude_oil_scraping,
    op_kwargs = {'start': '2018-01-02', 'end': curr_date},
    dag = dag
)

############################
# Extract Stage (Updating) #
############################
update_ticker_scraping = PythonOperator(
    task_id = 'update_ticker_scraping_data',
    python_callable = get_stock_price,
    # Need to fix op_kwargs to get the date as required
    op_kwargs = {'end': curr_date, 'df': tickers_df},
    dag = dag
)

update_exchange_scraping = PythonOperator(
    task_id = 'update_exchange_scraping_data',
    python_callable = exchange_rate_scraping,
    op_kwargs = {'start': '2018-01-02'},
    dag = dag
)

update_interest_scraping = PythonOperator(
    task_id = 'update_interest_scraping_data',
    python_callable = interest_rate_scraping,
    op_kwargs = {'start': '2018-01-02'},
    dag = dag
)

##################
# Pushing to GCS #
##################

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
)

# Push Crude Oil Prices to Cloud
oil_cloud = PythonOperator(
    task_id = 'push_crude_oil_cloud_data',
    python_callable = push_crude_oil,
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
)

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

##################
# Transformation #
##################
# Remove Duplicates exchange rate
distinct_exchange = BigQueryOperator(
    task_id = 'distinct_exchange_task',
    use_legacy_sql = False,
    sql = f'''
            create or replace table `{PROJECT_ID}.{STAGING_DATASET}.distinct_exchange_rate` 
            as select distinct parse_timestamp('%Y-%m-%d', end_of_day) as Date, *
            except (
                end_of_day
            )
            from `{PROJECT_ID}.{STAGING_DATASET}.init_exchange_rates`
    ''',
    dag = dag
)

# Reformat and remove duplicates interest rate
distinct_interest = BigQueryOperator(
    task_id = 'distinct_interest_task',
    use_legacy_sql = False,
    sql = f'''
    create or replace table `{PROJECT_ID}.{STAGING_DATASET}.distinct_interest_rate` 
    as select distinct parse_timestamp('%Y-%m-%d', end_of_day) as Date, *
    except(
        end_of_day,
        interbank_overnight, interbank_1w, interbank_1m, interbank_2m, interbank_3m,
        interbank_6m, interbank_12m, commercial_bills_3m, usd_sibor_3m, sgs_repo_overnight_rate
    )
    from `{PROJECT_ID}.{STAGING_DATASET}.init_interest_rates`
    ''',
    dag = dag
)

# Remove Duplicates
distinct_stock_prices = BigQueryOperator(
    task_id = 'distinct_stock_prices_task',
    use_legacy_sql = False,
    sql = f'''
    create or replace table `{PROJECT_ID}.{STAGING_DATASET}.distinct_hist_stock_prices` 
    as select distinct *
    from `{PROJECT_ID}.{STAGING_DATASET}.init_hist_stock_prices`
    ''',
    dag = dag
)

# Add SMA to df
sma_stock = PythonOperator(
    task_id = 'sma_stock_task',
    python_callable = sma_prices,
    dag = dag
)

# Load sma data from GCS to BQ
load_sma = GoogleCloudStorageToBigQueryOperator(
    task_id = 'stage_sma_task',
    bucket = 'stock_prediction_is3107',
    source_objects = ['final_stock.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.final_hist_prices',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

###########
# Loading #
###########
create_stocks_data = BigQueryOperator(
    task_id = 'create_stocks_data',
    use_legacy_sql = False,
    params = {
        'project_id': PROJECT_ID,
        'staging_dataset': STAGING_DATASET,
        'dwh_dataset': DWH_DATASET
    },
    sql = './sql/F_stock.sql',
    dag = dag
)

create_d_exchange = BigQueryOperator(
    task_id = 'create_d_exchange',
    use_legacy_sql = False,
    params = {
        'project_id': PROJECT_ID,
        'staging_dataset': STAGING_DATASET,
        'dwh_dataset': DWH_DATASET
    },
    sql = './sql/D_exchange_rate.sql',
    dag = dag
)

create_d_interest = BigQueryOperator(
    task_id = 'create_d_interest',
    use_legacy_sql = False,
    params = {
        'project_id': PROJECT_ID,
        'staging_dataset': STAGING_DATASET,
        'dwh_dataset': DWH_DATASET
    },
    sql = './sql/D_interest_rate.sql',
    dag = dag
)

# Start of DAG (to test)
start_init = BashOperator(
    task_id = 'start_task',
    bash_command = 'echo start',
    dag = dag
)

# Echo task finish (filler) (for testing dag paths)
finish_start = BashOperator(
    task_id = 'finish_task',
    bash_command = 'echo finish',
    trigger_rule = 'none_failed_or_skipped',
    dag = dag
)

run_stock_daily = BashOperator(
    task_id = 'run_stock_daily_task',
    bash_command = 'echo start',
    dag = dag
)

run_ex_daily = BashOperator(
    task_id = 'run_ex_daily_task',
    bash_command = 'echo start',
    dag = dag
)

run_int_daily = BashOperator(
    task_id = 'run_int_daily_task',
    bash_command = 'echo start',
    dag = dag
)

run_gold_daily = BashOperator(
    task_id = 'run_gold_daily_task',
    bash_command = 'echo start',
    dag = dag
)

run_silver_daily = BashOperator(
    task_id = 'run_silver_daily_task',
    bash_command = 'echo start',
    dag = dag
)

run_crude_oil_daily = BashOperator(
    task_id = 'run_crude_oil_daily_task',
    bash_command = 'echo start',
    dag = dag
)

recent_date = PythonOperator(
    task_id = 'recent_date_task',
    python_callable = get_recent_date,
    provide_context = True,
    dag = dag
)

############################
# Define Tasks Hierarchy   #
############################
start_init >> [stock_file_check, ex_file_check, int_file_check, gold_file_check, silver_file_check, crude_oil_file_check]
stock_file_check >> stock_path >> [ticker_scraping, run_stock_daily]
ex_file_check >> ex_path >> [exchange_rate_scraping, run_ex_daily]
int_file_check >> int_path >> [interest_rate_scraping, run_int_daily]
gold_file_check >> gold_path >> [gold_scraping, run_gold_daily]
silver_file_check >> silver_path >> [silver_scraping, run_silver_daily]
crude_oil_file_check >> crude_oil_path >> [crude_oil_scraping, run_crude_oil_daily]

ticker_scraping >> stock_cloud >> load_stock_prices >> distinct_stock_prices >> sma_stock >> load_sma
interest_rate_scraping >> interest_cloud >> load_interest_rates >> distinct_interest
exchange_rate_scraping >> exchange_cloud >> load_exchange_rates >> distinct_exchange
gold_scraping >> gold_cloud >> load_gold
silver_scraping >> silver_cloud >> load_silver
crude_oil_scraping >> oil_cloud >> load_crude_oil

[run_stock_daily, run_ex_daily, run_int_daily] >> recent_date >> update_ticker_scraping

[load_sma, distinct_interest, distinct_exchange] >> create_stocks_data
create_stocks_data >> [create_d_exchange, create_d_interest] >> finish_start
[load_gold, load_silver, load_crude_oil] >> finish_start