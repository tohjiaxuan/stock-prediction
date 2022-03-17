from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
import cchardet
import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib.request
import os
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'




default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email': ['user@gmail.com'],
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 0,
     'start_date': datetime(2022, 3, 6)
    }




dag = DAG(
    dag_id = 'testing_gcs',
    description = 'Scraping and collecting financial data of companies',
    default_args = default_args,
    schedule_interval = "@yearly",
    catchup = False
)


############################
# Define Python Functions  #
############################


# tickers_df wil be global
tickers_df = pd.read_csv('/home/airflow/airflow/dags/sti.csv')


# Function to execute query (Postgresql)
def execute_query_with_hook(query):
    hook = PostgresHook(postgres_conn_id="postgres_local")
    hook.run(query)

# Universal helper function applicable to all financial data
def cond(x):
    if x.startswith("hide"):
        return False
    else:
        return True

# Universal helper function applicable to all financial data
def replace_dash(list_needed):
    return ['0' if item == '-' else item for item in list_needed]

#################################
# Helper Functions for Scrape   #
# (Initialisation - Historical) #
#################################

# Helper function for scraping of net income
def table_content_income(ticker, table, df, col_names):
    table_body = table.find('tbody')
    rows = table_body.find_all('tr', {'class': cond})
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
        filter_cols = list(filter(None, cols))
        if not filter_cols:
            continue
        
        if filter_cols[0] == 'Net Income':
            final_cols = replace_dash(filter_cols)
            com_final_cols = [ticker] + final_cols
            new_df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
    return new_df

# Helper function for scraping of assets
def table_content_assets(ticker, table, df, col_names):
    table_body = table.find('tbody')
    rows = table_body.find_all('tr', {'class': cond})
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
        filter_cols = list(filter(None, cols))
        if not filter_cols:
            continue
        
        if filter_cols[0] == 'Total Assets':
            final_cols = replace_dash(filter_cols)
            com_final_cols = [ticker] + final_cols
            new_df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
    return new_df

# Helper function for scraping of liabilities
def table_content_liab(ticker, table, df, col_names):
    table_body = table.find('tbody')
    rows = table_body.find_all('tr', {'class': cond})
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
        filter_cols = list(filter(None, cols))
        if not filter_cols:
            continue
        
        if filter_cols[0] == 'Total Liabilities':
            final_cols = replace_dash(filter_cols)
            com_final_cols = [ticker] + final_cols
            new_df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
    return new_df


# Helper function for scraping of equity
def table_content_equity(ticker, table, df, col_names):
    table_body = table.find('tbody')
    rows = table_body.find_all('tr', {'class': cond})
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
        filter_cols = list(filter(None, cols))
        if not filter_cols:
            continue
        
        if filter_cols[0] == "Total Shareholders' Equity":
            final_cols = replace_dash(filter_cols)
            com_final_cols = [ticker] + final_cols
            new_df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
    return new_df

# Helper function for scraping of dividends
def table_content_div(ticker, table, df, col_names):
    table_body = table.find('tbody')
    rows = table_body.find_all('tr', {'class': cond})
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
        filter_cols = list(filter(None, cols))
        #print(filter_cols)
        if not filter_cols:
            continue
        if filter_cols[0] == "Cash Dividends Paid - Total":
            final_cols = replace_dash(filter_cols)
            com_final_cols = [ticker] + final_cols
            #new_df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
            df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
    
    return df




#################################
# Functions for Scrape          #
# (Initialisation - Historical) #
#################################

# Function for (1) scraping net income and (2) storing into gcs
def income_scraping_data(**kwargs):
    table_header = ['Ticker', 'Net Income', 'Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']
    df = pd.DataFrame(columns=table_header)
    wsj_start = 'https://www.wsj.com/market-data/quotes/SG/'
    wsj_end = '/financials/annual/income-statement'
    no_data = []
    for i in tickers_df['Symbol']:
    #for i in ['1A1','1A4', 'D05']:
        #   print("Starting collection for:", i)
        try: 
            wsj_url = wsj_start + i + wsj_end
            response = requests.get(wsj_url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            wants = soup.find_all("tr")
            table = soup.find('table', attrs={'class':'cr_dataTable'})
            df = table_content_income(i, table, df, table_header)
            #print("Collection for: ", i, " done")
            #print("---")
            
        except AttributeError as e:
            no_data.append(i)
            #print("No data available for: ", i)
            #print("---")

    # Remove 2021 from the Initialisation code so that we can simulate the yearly path using 2021 data. 
    df.drop('Year2021', inplace=True, axis=1)
    df[['Year2020', 'Year2019', 'Year2018', 'Year2017']] = df[['Year2020', 'Year2019', 'Year2018', 'Year2017']].astype(float)
    #netincome_parq = df.to_parquet('netincome.parquet')

    # upload to GCS:
    #df.to_parquet('gs://stock_prediction_is3107/netincome_init.parquet')
    return df
    


# Function for (1) scraping assets and (2) storing into gcs
def assets_scraping_data(**kwargs):
    table_header_assets = ['Ticker', 'Total Assets', 'Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']
    df_assets = pd.DataFrame(columns=table_header_assets)
    wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
    wsj_end_bs = '/financials/annual/balance-sheet'
    no_data_assets = []
    for i in tickers_df['Symbol']:
    #for i in ['1A1','1A4', 'D05']:
    #print("Starting collection for:", i)
        try: 
            wsj_url = wsj_start_bs + i + wsj_end_bs
            response = requests.get(wsj_url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            wants = soup.find_all("tr")
            table = soup.find('table', attrs={'class':'cr_dataTable'})
            df_assets = table_content_assets(i, table, df_assets, table_header_assets)
            #print("Collection for: ", i, " done")
            #print("---")
            
        except AttributeError as e:
            no_data_assets.append(i)
            #print("No data available for: ", i)
            #print("---")

    # Remove 2021 from the Initialisation code so that we can simulate the yearly path using 2021. 
    df_assets.drop('Year2021', inplace=True, axis=1)
    df_assets[['Year2020', 'Year2019', 'Year2018', 'Year2017']] = df_assets[['Year2020', 'Year2019', 'Year2018', 'Year2017']].astype(float)
    #assets_parq = df_assets.to_parquet('assets.parquet')

    # upload to GCS:
    #df_assets.to_parquet('gs://stock_prediction_is3107/assets_init.parquet')
    return df_assets


# Function for (1) scraping liabilities and (2) storing into gcs
def liab_scraping_data(**kwargs):
    table_header_liab = ['Ticker', 'Total Liabilities', 'Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']
    df_liab = pd.DataFrame(columns=table_header_liab)
    wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
    wsj_end_bs = '/financials/annual/balance-sheet'
    no_data_liab = []
    for i in tickers_df['Symbol']:
    #for i in ['1A1','1A4', 'D05']:
        #print("Starting collection for:", i)
        try: 
            wsj_url = wsj_start_bs + i + wsj_end_bs
            response = requests.get(wsj_url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            wants = soup.find('div', class_='collapsed')
            table = wants.find('table', attrs={'class':'cr_dataTable'})
            df_liab = table_content_liab(i, table, df_liab, table_header_liab)
            #print("Collection for: ", i, " done")
            #print("---")
            
        except AttributeError as e:
            no_data_liab.append(i)
            #print("No data available for: ", i)
            #print("---")
    
    # Remove 2021 from the Initialisation code so that we can simulate the yearly path using 2021. 
    df_liab.drop('Year2021', inplace=True, axis=1)
    df_liab[['Year2020', 'Year2019', 'Year2018', 'Year2017']] = df_liab[['Year2020', 'Year2019', 'Year2018', 'Year2017']].astype(float) 
    #liab_parq = df_liab.to_parquet('liab.parquet')

    # upload to GCS:
    #df_liab.to_parquet('gs://stock_prediction_is3107/liab_init.parquet')
    
    return df_liab

# Function for (1) scraping equity and (2) storing into gcs
def equity_scraping_data(**kwargs):
    table_header_eq = ['Ticker', 'Total Shareholders Equity', 'Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']
    df_eq = pd.DataFrame(columns=table_header_eq)
    wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
    wsj_end_bs = '/financials/annual/balance-sheet'
    no_data_eq = []
    for i in tickers_df['Symbol']:
    #for i in ['1A1','1A4', 'D05']:
        #print("Starting collection for:", i)
        try: 
            wsj_url = wsj_start_bs + i + wsj_end_bs
            response = requests.get(wsj_url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            wants = soup.find('div', class_='collapsed')
            table = wants.find('table', attrs={'class':'cr_dataTable'})
            df_eq = table_content_equity(i, table, df_eq, table_header_eq)
            #print("Collection for: ", i, " done")
            #print("---")
            
        except AttributeError as e:
            no_data_eq.append(i)
            #print("No data available for: ", i)
            #print("---")
    
    # Remove 2021 from the Initialisation code so that we can simulate the yearly path using 2021. 
    df_eq.drop('Year2021', inplace=True, axis=1)
    df_eq[['Year2020', 'Year2019', 'Year2018', 'Year2017']] = df_eq[['Year2020', 'Year2019', 'Year2018', 'Year2017']].astype(float)
    #equity_parq = df_eq.to_parquet('equity.parquet')

    # upload to GCS:
    #df_eq.to_parquet('gs://stock_prediction_is3107/equity_init.parquet')

    return df_eq

# Function for (1) scraping dividends and (2) storing into gcs
def dividends_scraping_data(**kwargs):
    table_header_div = ['Ticker', 'Total Cash Dividends', 'Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']
    df_div = pd.DataFrame(columns=table_header_div)
    wsj_start_cf = 'https://www.wsj.com/market-data/quotes/SG/'
    wsj_end_cf = '/financials/annual/cash-flow'
    no_data_div = []
    #for i in ['1A1','1A4', 'D05']:
    for i in tickers_df['Symbol']:
        #print("Starting collection for:", i)
        try: 
            wsj_url = wsj_start_cf + i + wsj_end_cf
            response = requests.get(wsj_url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            wants = soup.find_all('div', class_='collapsed')
            temp = None
            for want in wants:
                temp = want
            table = temp.find('table', attrs={'class':'cr_dataTable'})
            df_div = table_content_div(i, table, df_div, table_header_div)
            #print("Collection for: ", i, " done")
            #print("---")
            
        except AttributeError as e:
            no_data_div.append(i)
            #print("No data available for: ", i)
            #print("---")
    
    # Remove 2021 from the Initialisation code so that we can simulate the yearly path using 2021. 
    df_div.drop('Year2021', inplace=True, axis=1)
    df_div[['Year2020', 'Year2019', 'Year2018', 'Year2017']] = df_div[['Year2020', 'Year2019', 'Year2018', 'Year2017']].astype(float)
    #div_parq = df_div.to_parquet('div.parquet')

    # upload to GCS:
    #df_div.to_parquet('gs://stock_prediction_is3107/div_init.parquet')

    return df_div

# Function for (1) scraping inflation
def inflation_scraping_data(**kwargs):
    url = 'https://www.rateinflation.com/inflation-rate/singapore-inflation-rate/'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "lxml")
    rows = soup.find_all('tr')
    final = []
    for row in rows:
        col = row.find_all('td')
        col = [ele.text.replace('%', '') for ele in col]
        final.append(col[::len(col)-1])
    init = final[3:6]
    df = pd.DataFrame(init, columns=['Year', 'Inflation'])
    return df





############################
# Define Airflow Operators #
############################


# Create table to store inflation rate (inflation)


# Create table to store net income (netincome)



# Create table to store assets (assets)

# Create table to store liabilities (liability)


# Create table to store equity (equity)

# Create table to store dividends (dividends)




start_pipeline = DummyOperator(
    task_id = 'start_pipeline',
    dag = dag
)


#################################
# Airflow Operators             #
# (Initialisation - Historical) #
#################################

scrape_init = DummyOperator(
    task_id = 'scrape_init',
    dag = dag
)

# Scraping annual income 
income_scraping = PythonOperator(
    task_id = 'income_scraping_data',
    python_callable = income_scraping_data,
    dag = dag
)

# Scraping annual assets
assets_scraping = PythonOperator(
    task_id = 'assets_scraping_data',
    python_callable = assets_scraping_data,
    dag = dag
)

# Scraping annual liab
liab_scraping = PythonOperator(
    task_id = 'liab_scraping_data',
    python_callable = liab_scraping_data,
    dag = dag
)

# Scraping annual equity
equity_scraping = PythonOperator(
    task_id = 'equity_scraping_data',
    python_callable = equity_scraping_data,
    dag = dag
)

# Scraping annual dividends
dividends_scraping = PythonOperator(
    task_id = 'dividends_scraping_data',
    python_callable = dividends_scraping_data,
    dag = dag
)

# Scraping annual inflation
inflation_scraping = PythonOperator(
    task_id = 'inflation_scraping_data',
    python_callable = inflation_scraping_data,
    dag = dag
)






############################
# Push to GCS From XCOMS #
############################

# Pull data from XCOMs for financials and push to cloud
def push_financials_init(ti):

    netincome_init = ti.xcom_pull(task_ids='income_scraping_data')
    netincome_init.to_parquet('gs://stock_prediction_is3107/netincome_init.parquet')

    assets_init = ti.xcom_pull(task_ids='assets_scraping_data')
    assets_init.to_parquet('gs://stock_prediction_is3107/assets_init.parquet')

    liab_init = ti.xcom_pull(task_ids='liab_scraping_data')
    liab_init.to_parquet('gs://stock_prediction_is3107/liab_init.parquet')

    eq_init = ti.xcom_pull(task_ids='equity_scraping_data')
    eq_init.to_parquet('gs://stock_prediction_is3107/equity_init.parquet')

    div_init = ti.xcom_pull(task_ids='dividends_scraping_data')
    div_init.to_parquet('gs://stock_prediction_is3107/div_init.parquet')

    inflation_init = ti.xcom_pull(task_ids='inflation_scraping_data')
    inflation_init.to_parquet('gs://stock_prediction_is3107/inflation_init.parquet')

# Operator to push to cloud
financials_cloud_init = PythonOperator(
    task_id = 'financials_cloud_init',
    python_callable = push_financials_init,
    dag = dag
)

##############
# Staging    #
##############

# Load net income from GCS to BQ
stage_netincome_init = GCSToBigQueryOperator(
    task_id = 'stage_netincome_init',
    bucket = 'stock_prediction_is3107',
    source_objects = ['netincome_init.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.netincome_init',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load assets from GCS to BQ
stage_assets_init = GCSToBigQueryOperator(
    task_id = 'stage_assets_init',
    bucket = 'stock_prediction_is3107',
    source_objects = ['assets_init.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.assets_init',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load liab from GCS to BQ
stage_liab_init = GCSToBigQueryOperator(
    task_id = 'stage_liab_init',
    bucket = 'stock_prediction_is3107',
    source_objects = ['liab_init.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.liab_init',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load equity from GCS to BQ
stage_equity_init = GCSToBigQueryOperator(
    task_id = 'stage_equity_init',
    bucket = 'stock_prediction_is3107',
    source_objects = ['equity_init.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.equity_init',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load div from GCS to BQ
stage_div_init = GCSToBigQueryOperator(
    task_id = 'stage_div_init',
    bucket = 'stock_prediction_is3107',
    source_objects = ['div_init.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.div_init',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load inflation from GCS to BQ
stage_inflation_init = GCSToBigQueryOperator(
    task_id = 'stage_inflation_init',
    bucket = 'stock_prediction_is3107',
    source_objects = ['inflation_init.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.inflation_init',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

############################
# Define Tasks Hierarchy   #
############################

#start_pipeline >> choose_best_path
#choose_best_path >> scrape_init >> [income_scraping, assets_scraping, liab_scraping, equity_scraping, dividends_scraping] >> finish_init
#choose_best_path >> scrape_yearly >> [income_scraping_yearly, assets_scraping_yearly, liab_scraping_yearly, equity_scraping_yearly, dividends_scraping_yearly] >> finish_yearly

start_pipeline >> scrape_init >> [income_scraping, assets_scraping, liab_scraping, equity_scraping, dividends_scraping, inflation_scraping] 
[income_scraping, assets_scraping, liab_scraping, equity_scraping, dividends_scraping, inflation_scraping] >> financials_cloud_init
financials_cloud_init >> [stage_netincome_init, stage_assets_init, stage_liab_init, stage_equity_init, stage_div_init, stage_inflation_init]