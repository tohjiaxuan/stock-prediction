from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from google.cloud import storage

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, InvalidArgumentException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from xvfbwrapper import Xvfb

import json
import os
import pandas as pd
import requests
import time
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

# Function to execute query (Postgresql)
def execute_query_with_hook(query):
    hook = PostgresHook(postgres_conn_id="postgres_local")
    hook.run(query)

# Function for (1) scraping yahoo finance news and (2) storing into psql table
def yahoofinance_scraping_data(**kwargs):

    vdisplay = Xvfb()
    vdisplay.start()

    # Chrome driver
    # Select custom Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument('--headless') 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(ChromeDriverManager().install(), options = options)

    # scrape news for each ticker
    start = 'https://sg.finance.yahoo.com/quote/'
    end = '/news'
    df_final = pd.DataFrame(columns =['Ticker', 'Title', 'Date', 'Link'])
    
    for ticker in tickers_df['New Symbol']:
        #   print("Starting collection for:", i)
        try: 
            link = start + ticker + end
            driver.get(link)
            time.sleep(10)
            print('-----------------')
            print(ticker)

            # scrolling
            height = driver.execute_script("return document.documentElement.scrollHeight")
            for i in range(height):
                driver.execute_script('window.scrollBy(0,20)') # scroll by 20 on each iteration
                height = driver.execute_script("return document.documentElement.scrollHeight") 
                # reset height to the new height after scroll-triggered elements have been loaded. 
            # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li[*]/div/div/div[*]/h3/a')))
            test = driver.find_elements(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li[*]/div/div/div[*]/h3/a')
            for i in range(len(test)):
                
                link = ''
                date = ''
                title = ''
                index = i+1

                # get links
                elems_links = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                link =elems_links.get_attribute("href")
                #links = [my_elem.get_attribute("href") for my_elem in elems_links]

                # get titles
                elems_titles = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                title =elems_titles.text
                #titles = [my_elem.text for my_elem in elems_titles]

                # get dates
                try:
                    elems_dates = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/div/span[2]')
                    date = elems_dates.text
                except NoSuchElementException as e:
                    pass

                finally: 
                    
                    row = [ticker, title, date, link]
                    df_final.loc[len(df_final)] = row

        except TimeoutException as e:
            pass
        except WebDriverException as e:
            print('webdriver exception')
    print('scrape successfully')

    df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
    df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')

    # remove row if it is ad
    df_final.replace("", np.nan, inplace=True)
    df_final.dropna(subset=['Date'], inplace = True)
    df_final['Source'] = 'Yahoo Finance News'
    df_final['Comments'] = ''


    # Clean up Updated SG Time information (put in terms of date)
    cleaned_time = []
    current_time = datetime.today()
    for t in df_final['Date']:
        if 'minute' in t:
            last_update = current_time - timedelta(minutes=int(t.split(' ')[0]))
        elif 'hour' in t:
            last_update = current_time - timedelta(hours=int(t.split(' ')[0]))
        elif 'day' in t:
            if 'yesterday' in t:
                last_update = current_time - timedelta(days=int(1))
            else:
                last_update = current_time - timedelta(days=int(t.split(' ')[0]))
        elif 'month' in t:
            if 'last' in t:
                last_update = current_time + relativedelta(months=-1)
            else:
                date = int(t.split(' ')[0])
                last_update = current_time + relativedelta(months=-date)
        elif 'year' in t:
            if 'last' in t:
                last_update = current_time + relativedelta(years=-1)
            else:
                date = int(t.split(' ')[0])
                last_update = current_time + relativedelta(years=-date)
        else:
            last_update = t # will need to double check
            print('check', t)
            
        #last_update = last_update.strftime('%d/%m/%Y %H:%M %p') # 27/02/2022 09:05 AM
        last_update = last_update.strftime('%Y/%m/%d') 
        cleaned_time.append(last_update)
    df_final['Date'] = cleaned_time   
    df_final.reset_index(drop=True)  
    print('clean successfully')   

    for i in range(len(df_final)):

        print('this is query')
        row = df_final.loc[i,:]
        print(row)

        query = f'''
        INSERT INTO yahoofinance (ticker, date, title, link, source, comments)
        VALUES ('{row["Ticker"]}', DATE '{row["Date"]}', '{row["Title"]}', '{row["Link"]}', '{row["Source"]}', '{row["Comments"]}');
        '''
        print(query)
        execute_query_with_hook(query)
    print('queries successful')

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
# Create table to store yahoo finance news
create_table_yahoofinance = PostgresOperator (
    task_id = 'create_table_yahoofinance',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS yahoofinance (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        title TEXT NOT NULL,
        link TEXT NOT NULL,
        source TEXT,
        comments TEXT
        );
        '''
    )

# Scraping yahoo finance news
yahoofinance_scraping = PythonOperator(
    task_id = 'yahoofinance_scraping_data',
    python_callable = yahoofinance_scraping_data,
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
start_init >> create_table_yahoofinance

create_table_yahoofinance >> [ticker_scraping, exchange_rate_scraping, interest_rate_scraping, yahoofinance_scraping]

[ticker_scraping, exchange_rate_scraping, interest_rate_scraping, yahoofinance_scraping] >> finish_start