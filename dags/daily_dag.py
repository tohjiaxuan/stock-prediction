from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from bs4 import BeautifulSoup
from datetime import datetime

import pandas as pd
import requests
import urllib.request
import json
import yfinance as yf 

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}

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
    print(df.head(5))
    return df

# Choose between initialisation and daily path
def choose_path(**kwargs):
    # Connect to google cloud storage
    # Fetch to see if google cloud storage has the table already
    print("dummy")


def finish_init(**kwargs):
    print('Initialisation Done')


####################
# Define Operators #
####################

ticker_scraping = PythonOperator(
    task_id = 'ticker_scraping_data',
    python_callable =  get_stock_price,
    # Need to fix op_kwargs to get the date as required
    op_kwargs = {'start':'2018-01-01', 'end':'2022-03-13', 'df': tickers_df},
    dag = dag
)

choose_best_path = BranchPythonOperator(
    task_id = 'choose_path',
    python_callable = choose_path,
    dag = dag
)

finish_start_init = PythonOperator(
    task_id = 'finish_start_init',
    python_callable = finish_init,
    dag = dag
)


############################
# Define Tasks Hierarchy   #
############################
ticker_scraping >> finish_start_init