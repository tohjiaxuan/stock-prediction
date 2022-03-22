from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, date
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
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


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
    dag_id = 'financial_scrape',
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
# Helper Functions for Scrape   #
# (Yearly)                      #
#################################

# Helper function for scraping of net income
def table_content_income_yearly(ticker, table, df, col_names):
    table_body = table.find('tbody')
    rows = table_body.find_all('tr', {'class': cond})
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
        filter_cols = list(filter(None, cols))
        if not filter_cols:
            continue
        
        if filter_cols[0] == 'Net Income':
            intm_cols = replace_dash(filter_cols)
            final_cols = intm_cols[0:2]
            com_final_cols = [ticker] + final_cols
            new_df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
    return new_df

# Helper function for scraping of assets
def table_content_assets_yearly(ticker, table, df, col_names):
    table_body = table.find('tbody')
    rows = table_body.find_all('tr', {'class': cond})
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
        filter_cols = list(filter(None, cols))
        if not filter_cols:
            continue
        
        if filter_cols[0] == 'Total Assets':
            intm_cols = replace_dash(filter_cols)
            final_cols = intm_cols[0:2]
            com_final_cols = [ticker] + final_cols
            new_df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
    return new_df

# Helper function for scraping of liabilities
def table_content_liab_yearly(ticker, table, df, col_names):
    table_body = table.find('tbody')
    rows = table_body.find_all('tr', {'class': cond})
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
        filter_cols = list(filter(None, cols))
        if not filter_cols:
            continue
        
        if filter_cols[0] == 'Total Liabilities':
            intm_cols = replace_dash(filter_cols)
            final_cols = intm_cols[0:2]
            com_final_cols = [ticker] + final_cols
            new_df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
    return new_df


# Helper function for scraping of equity
def table_content_equity_yearly(ticker, table, df, col_names):
    table_body = table.find('tbody')
    rows = table_body.find_all('tr', {'class': cond})
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
        filter_cols = list(filter(None, cols))
        if not filter_cols:
            continue
        
        if filter_cols[0] == "Total Shareholders' Equity":
            intm_cols = replace_dash(filter_cols)
            final_cols = intm_cols[0:2]
            com_final_cols = [ticker] + final_cols
            new_df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
    return new_df

# Helper function for scraping of dividends
def table_content_div_yearly(ticker, table, df, col_names):
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
            intm_cols = replace_dash(filter_cols)
            final_cols = intm_cols[0:2]
            com_final_cols = [ticker] + final_cols
            print('this is com_final_coms')
            print(com_final_cols)
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

#################################
# Functions for Scrape          #
# Yearly                        #
#################################

# Function for (1) scraping net income and (2) storing into gcs
def income_scraping_data_yearly(**kwargs):
    # only the previous year's data will be available (the current year's eg 2022's data will not be available yet in 2022.)
    # hence, we minus 1 from the current year to obtain the yearly data. 
    curr_year = str(date.today().year - 1)

    table_header = ['Ticker', 'Net Income', 'Year'+curr_year]
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
            df = table_content_income_yearly(i, table, df, table_header)
            #print("Collection for: ", i, " done")
            #print("---")
            
        except AttributeError as e:
            no_data.append(i)
            #print("No data available for: ", i)
            #print("---")

    #netincome_yearly_parq = df.to_parquet('netincome_yearly.parquet')

    # upload to GCS:
    #df.to_parquet('gs://stock_prediction_is3107/netincome_yearly.parquet')
    x = 'Year'+curr_year
    df[x] = df[x].astype(float)
    return df


# Function for (1) scraping assets and (2) storing into gcs
def assets_scraping_data_yearly(**kwargs):
    # only the previous year's data will be available (the current year's eg 2022's data will not be available yet in 2022.)
    # hence, we minus 1 from the current year to obtain the yearly data.
    curr_year = str(date.today().year - 1)

    table_header_assets = ['Ticker', 'Total Assets', 'Year'+curr_year]
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
            df_assets = table_content_assets_yearly(i, table, df_assets, table_header_assets)
            #print("Collection for: ", i, " done")
            #print("---")
            
        except AttributeError as e:
            no_data_assets.append(i)
            #print("No data available for: ", i)
            #print("---")

    
    #assets_yearly_parq = df_assets.to_parquet('assets_yearly.parquet')

    # upload to GCS:
    #df_assets.to_parquet('gs://stock_prediction_is3107/assets_yearly.parquet')
    x = 'Year'+curr_year
    df_assets[x] = df_assets[x].astype(float)
    return df_assets


# Function for (1) scraping liabilities and (2) storing into gcs
def liab_scraping_data_yearly(**kwargs):
    # only the previous year's data will be available (the current year's eg 2022's data will not be available yet in 2022.)
    # hence, we minus 1 from the current year to obtain the yearly data.
    curr_year = str(date.today().year - 1)

    table_header_liab = ['Ticker', 'Total Liabilities', 'Year'+curr_year]
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
            df_liab = table_content_liab_yearly(i, table, df_liab, table_header_liab)
            #print("Collection for: ", i, " done")
            #print("---")
            
        except AttributeError as e:
            no_data_liab.append(i)
            #print("No data available for: ", i)
            #print("---")
    
    
    #liab_yearly_parq = df_liab.to_parquet('liab_yearly.parquet')

    # upload to GCS:
    #df_liab.to_parquet('gs://stock_prediction_is3107/liab_yearly.parquet')
    x = 'Year'+curr_year
    df_liab[x] = df_liab[x].astype(float)
    return df_liab

# Function for (1) scraping equity and (2) storing into gcs
def equity_scraping_data_yearly(**kwargs):
    # only the previous year's data will be available (the current year's eg 2022's data will not be available yet in 2022.)
    # hence, we minus 1 from the current year to obtain the yearly data.
    curr_year = str(date.today().year - 1)

    table_header_eq = ['Ticker', 'Total Shareholders Equity', 'Year'+curr_year]
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
            df_eq = table_content_equity_yearly(i, table, df_eq, table_header_eq)
            #print("Collection for: ", i, " done")
            #print("---")
            
        except AttributeError as e:
            no_data_eq.append(i)
            #print("No data available for: ", i)
            #print("---")
    
    
    #equity_yearly_parq = df_eq.to_parquet('equity_yearly.parquet')

    # upload to GCS:
    #df_eq.to_parquet('gs://stock_prediction_is3107/equity_yearly.parquet')
    x = 'Year'+curr_year
    df_eq[x] = df_eq[x].astype(float)
    return df_eq

# Function for (1) scraping dividends and (2) storing into gcs
def dividends_scraping_data_yearly(**kwargs):
   # only the previous year's data will be available (the current year's eg 2022's data will not be available yet in 2022.)
    # hence, we minus 1 from the current year to obtain the yearly data.
    curr_year = str(date.today().year - 1)

    table_header_div = ['Ticker', 'Total Cash Dividends', 'Year'+curr_year]
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
            df_div = table_content_div_yearly(i, table, df_div, table_header_div)
            #print("Collection for: ", i, " done")
            #print("---")
            
        except AttributeError as e:
            no_data_div.append(i)
            #print("No data available for: ", i)
            #print("---")
    
  
    #div_yearly_parq = df_div.to_parquet('div_yearly.parquet')

    # upload to GCS:
    #df_div.to_parquet('gs://stock_prediction_is3107/div_yearly.parquet')
    x = 'Year'+curr_year
    df_div[x] = df_div[x].astype(float)
    return df_div

# Function for (1) scraping inflation
def inflation_scraping_data_yearly(**kwargs):
    url = 'https://www.rateinflation.com/inflation-rate/singapore-inflation-rate/'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "lxml")
    rows = soup.find_all('tr')
    final = []
    for row in rows:
        col = row.find_all('td')
        col = [ele.text.replace('%', '') for ele in col]
        final.append(col[::len(col)-1])
    yearly = []
    for item in final:
        if len(item) != 0:
            # only the previous year's data will be available (the current year's eg 2022's data will not be available yet in 2022.)
            # hence, we minus 1 from the current year to obtain the yearly data.
            if (item[0] == str(date.today().year - 1)) and (len(item[1]) != 0):
                yearly.append(item)
    df = pd.DataFrame(yearly, columns=['Year', 'Inflation'])
    return df




############################
# Define Airflow Operators #
############################


start_pipeline = DummyOperator(
    task_id = 'start_pipeline',
    dag = dag
)


#################################
# Airflow Operators             #
# (Initialisation - Historical) #
#################################

scrape_netincome_init = DummyOperator(
    task_id = 'scrape_netincome_init',
    dag = dag
)

scrape_assets_init = DummyOperator(
    task_id = 'scrape_assets_init',
    dag = dag
)

scrape_liab_init = DummyOperator(
    task_id = 'scrape_liab_init',
    dag = dag
)

scrape_equity_init = DummyOperator(
    task_id = 'scrape_equity_init',
    dag = dag
)

scrape_div_init = DummyOperator(
    task_id = 'scrape_div_init',
    dag = dag
)

scrape_inflation_init = DummyOperator(
    task_id = 'scrape_inflation_init',
    dag = dag
)


end_init = DummyOperator(
    task_id = 'end_init',
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


#################################
# Airflow Operators             #
# (Yearly)                      #
#################################

scrape_netincome_yearly = DummyOperator(
    task_id = 'scrape_netincome_yearly',
    dag = dag
)

scrape_assets_yearly = DummyOperator(
    task_id = 'scrape_assets_yearly',
    dag = dag
)

scrape_liab_yearly = DummyOperator(
    task_id = 'scrape_liab_yearly',
    dag = dag
)

scrape_equity_yearly = DummyOperator(
    task_id = 'scrape_equity_yearly',
    dag = dag
)

scrape_div_yearly = DummyOperator(
    task_id = 'scrape_div_yearly',
    dag = dag
)

scrape_inflation_yearly = DummyOperator(
    task_id = 'scrape_inflation_yearly',
    dag = dag
)

end_yearly = DummyOperator(
    task_id = 'end_yearly',
    dag = dag
)


# Scraping annual income 
income_scraping_yearly = PythonOperator(
    task_id = 'income_scraping_data_yearly',
    python_callable = income_scraping_data_yearly,
    dag = dag
)

# Scraping annual assets
assets_scraping_yearly = PythonOperator(
    task_id = 'assets_scraping_data_yearly',
    python_callable = assets_scraping_data_yearly,
    dag = dag
)

# Scraping annual liab
liab_scraping_yearly = PythonOperator(
    task_id = 'liab_scraping_data_yearly',
    python_callable = liab_scraping_data_yearly,
    dag = dag
)

# Scraping annual equity
equity_scraping_yearly = PythonOperator(
    task_id = 'equity_scraping_data_yearly',
    python_callable = equity_scraping_data_yearly,
    dag = dag
)

# Scraping annual dividends
dividends_scraping_yearly = PythonOperator(
    task_id = 'dividends_scraping_data_yearly',
    python_callable = dividends_scraping_data_yearly,
    dag = dag
)

# Scraping annual inflation
inflation_scraping_yearly = PythonOperator(
    task_id = 'inflation_scraping_data_yearly',
    python_callable = inflation_scraping_data_yearly,
    dag = dag
)


############################
# Push to GCS From XCOMS #
############################

##################
# Initialisation #
##################
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
# Yearly     #
##############

# Pull data from XCOMs for financials and push to cloud
def push_financials_yearly(ti):

    netincome_yearly = ti.xcom_pull(task_ids='income_scraping_data_yearly')
    netincome_yearly.to_parquet('gs://stock_prediction_is3107/netincome_yearly.parquet')

    assets_yearly = ti.xcom_pull(task_ids='assets_scraping_data_yearly')
    assets_yearly.to_parquet('gs://stock_prediction_is3107/assets_yearly.parquet')

    liab_yearly = ti.xcom_pull(task_ids='liab_scraping_data_yearly')
    liab_yearly.to_parquet('gs://stock_prediction_is3107/liab_yearly.parquet')

    eq_yearly = ti.xcom_pull(task_ids='equity_scraping_data_yearly')
    eq_yearly.to_parquet('gs://stock_prediction_is3107/equity_yearly.parquet')

    div_yearly = ti.xcom_pull(task_ids='dividends_scraping_data_yearly')
    div_yearly.to_parquet('gs://stock_prediction_is3107/div_yearly.parquet')

    inflation_yearly = ti.xcom_pull(task_ids='inflation_scraping_data_yearly')
    inflation_yearly.to_parquet('gs://stock_prediction_is3107/inflation_yearly.parquet')

# Operator to push to cloud
financials_cloud_yearly = PythonOperator(
    task_id = 'financials_cloud_yearly',
    python_callable = push_financials_yearly,
    dag = dag
)

##############
# Staging    #
##############

##################
# Initialisation #
##################

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

##################
# Yearly         #
##################

# Load net income from GCS to BQ
stage_netincome_yearly = GCSToBigQueryOperator(
    task_id = 'stage_netincome_yearly',
    bucket = 'stock_prediction_is3107',
    source_objects = ['netincome_yearly.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.netincome_yearly',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load assets from GCS to BQ
stage_assets_yearly = GCSToBigQueryOperator(
    task_id = 'stage_assets_yearly',
    bucket = 'stock_prediction_is3107',
    source_objects = ['assets_yearly.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.assets_yearly',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load liab from GCS to BQ
stage_liab_yearly = GCSToBigQueryOperator(
    task_id = 'stage_liab_yearly',
    bucket = 'stock_prediction_is3107',
    source_objects = ['liab_yearly.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.liab_yearly',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load equity from GCS to BQ
stage_equity_yearly = GCSToBigQueryOperator(
    task_id = 'stage_equity_yearly',
    bucket = 'stock_prediction_is3107',
    source_objects = ['equity_yearly.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.equity_yearly',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load div from GCS to BQ
stage_div_yearly = GCSToBigQueryOperator(
    task_id = 'stage_div_yearly',
    bucket = 'stock_prediction_is3107',
    source_objects = ['div_yearly.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.div_yearly',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

# Load inflation from GCS to BQ
stage_inflation_yearly = GCSToBigQueryOperator(
    task_id = 'stage_inflation_yearly',
    bucket = 'stock_prediction_is3107',
    source_objects = ['inflation_yearly.parquet'],
    destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.inflation_yearly',
    write_disposition='WRITE_TRUNCATE',
    autodetect = True,
    source_format = 'PARQUET',
    dag = dag
)

#####################
# Transformation in #
# Staging           #
#####################

##################
# Initialisation #
##################

# Reformat Tables, removes duplicates
reformat_netincome_init = BigQueryOperator(
    task_id = 'reformat_netincome_init',
    use_legacy_sql = False,
    sql = f'''
    create table `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_init` as select * from
    (select distinct * from ((select ticker, '2020' as year, year2020 as netincome from `{PROJECT_ID}.{STAGING_DATASET}.netincome_init`) 
        union all   
    (select ticker, '2019' as year, year2019 as netincome from `{PROJECT_ID}.{STAGING_DATASET}.netincome_init`) 
        union all 
    (select ticker, '2018' as year, year2018 as netincome from `{PROJECT_ID}.{STAGING_DATASET}.netincome_init`)))
    ''',
    dag = dag
)

reformat_assets_init = BigQueryOperator(
    task_id = 'reformat_assets_init',
    use_legacy_sql = False,
    sql = f'''
    create table `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_init` as select * from
    (select distinct * from ((select ticker, '2020' as year, year2020 as assets from `{PROJECT_ID}.{STAGING_DATASET}.assets_init`) 
        union all   
    (select ticker, '2019' as year, year2019 as assets from `{PROJECT_ID}.{STAGING_DATASET}.assets_init`) 
        union all 
    (select ticker, '2018' as year, year2018 as assets from `{PROJECT_ID}.{STAGING_DATASET}.assets_init`)))
    ''',
    dag = dag
)

reformat_liab_init = BigQueryOperator(
    task_id = 'reformat_liab_init',
    use_legacy_sql = False,
    sql = f'''
    create table `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_init` as select * from
    (select distinct * from ((select ticker, '2020' as year, year2020 as liability from `{PROJECT_ID}.{STAGING_DATASET}.liab_init`) 
        union all   
    (select ticker, '2019' as year, year2019 as liability from `{PROJECT_ID}.{STAGING_DATASET}.liab_init`) 
        union all 
    (select ticker, '2018' as year, year2018 as liability from `{PROJECT_ID}.{STAGING_DATASET}.liab_init`)))
    ''',
    dag = dag
)

reformat_equity_init = BigQueryOperator(
    task_id = 'reformat_equity_init',
    use_legacy_sql = False,
    sql = f'''
    create table `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_init` as select * from
    (select distinct * from ((select ticker, '2020' as year, year2020 as equity from `{PROJECT_ID}.{STAGING_DATASET}.equity_init`) 
        union all   
    (select ticker, '2019' as year, year2019 as equity from `{PROJECT_ID}.{STAGING_DATASET}.equity_init`) 
        union all 
    (select ticker, '2018' as year, year2018 as equity from `{PROJECT_ID}.{STAGING_DATASET}.equity_init`)))
    ''',
    dag = dag
)

reformat_div_init = BigQueryOperator(
    task_id = 'reformat_div_init',
    use_legacy_sql = False,
    sql = f'''
    create table `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_init` as select * from
    (select distinct * from ((select ticker, '2020' as year, year2020 as dividends from `{PROJECT_ID}.{STAGING_DATASET}.div_init`) 
        union all   
    (select ticker, '2019' as year, year2019 as dividends from `{PROJECT_ID}.{STAGING_DATASET}.div_init`) 
        union all 
    (select ticker, '2018' as year, year2018 as dividends from `{PROJECT_ID}.{STAGING_DATASET}.div_init`)))
    ''',
    dag = dag
)

# join the tables, ensure column types
join_financials = BigQueryOperator(
    task_id = 'join_financials',
    use_legacy_sql = False,
    sql = f'''
        create table `{PROJECT_ID}.{STAGING_DATASET}.financials_join` 
        (
            ticker string not null,
            year string not null,
            netincome float64, 
            assets float64, 
            liability float64, 
            equity float64, 
            dividends float64
        )
        as
        select iale.ticker, iale.year, iale.netincome, iale.assets, iale.liability, iale.equity, d.dividends
        from 
        (select ial.ticker, ial.year, ial.netincome, ial.assets, ial.liability, e.equity
        from 
        (select ia.ticker, ia.year, ia.netincome, ia.assets, l.liability 
        from (SELECT i.ticker, i.year, i.netincome, a.assets FROM `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_init` i
                left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_init` a 
                        on i.ticker = a.ticker and i.year = a.year) as ia 
                left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_init` l
                        on ia.ticker = l.ticker and ia.year = l.year) ial
                left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_init` e
                        on ial.ticker = e.ticker and ial.year = e.year) iale
                left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_init` d
                        on iale.ticker = d.ticker and iale.year = d.year
    ''',
    dag = dag
)


# Add financial ratio calculations
add_financial_ratios = BigQueryOperator(
    task_id = 'add_financial_ratios',
    use_legacy_sql = False,
    sql = f'''
            create table `{PROJECT_ID}.{STAGING_DATASET}.financials_with_ratios` 
            (
                ticker string not null,
                year timestamp not null,
                netincome float64, 
                assets float64, 
                liability float64, 
                equity float64, 
                yearlydividends float64,
                ROA float64,
                ROE float64,
                debttoequity float64,
                networth float64
            )
            as
            select concat(ticker, '.SI') as ticker, parse_timestamp("%Y-%m-%d", concat(year, '-12-31')) as year, netincome, assets, liability, equity, dividends,
            (case when netincome is null then null
            when netincome = 0 then 0
            when assets is null then null
            when assets = 0 then null 
            else netincome/assets end) as ROA, 

            (case when netincome is null then null
            when netincome = 0 then 0
            when equity is null then null
            when equity = 0 then null 
            else netincome/equity end) as ROE,

            (case when liability is null then null
            when liability = 0 then 0
            when equity is null then null
            when equity = 0 then null 
            else liability/equity end) as debttoequity, 

            (case when liability is null then null
            when assets is null then null
            else liability-assets end) as networth,  
            from
            `{PROJECT_ID}.{STAGING_DATASET}.financials_join`
    ''',
    dag = dag
)

##################
# Yearly         #
##################

# Reformat Tables, removes duplicates
reformat_netincome_yearly = BigQueryOperator(
    task_id = 'reformat_netincome_yearly',
    use_legacy_sql = False,
    sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_yearly` as
        (SELECT distinct ticker, '2021' as year, year2021 as netincome FROM `{PROJECT_ID}.{STAGING_DATASET}.netincome_yearly`)
    ''',
    dag = dag
)

reformat_assets_yearly = BigQueryOperator(
    task_id = 'reformat_assets_yearly',
    use_legacy_sql = False,
    sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_yearly` as
        (SELECT distinct ticker, '2021' as year, year2021 as assets FROM `{PROJECT_ID}.{STAGING_DATASET}.assets_yearly`)
    ''',
    dag = dag
)

reformat_liab_yearly = BigQueryOperator(
    task_id = 'reformat_liab_yearly',
    use_legacy_sql = False,
    sql = f'''
    create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_yearly` as
        (SELECT distinct ticker, '2021' as year, year2021 as liability FROM `{PROJECT_ID}.{STAGING_DATASET}.liab_yearly`)
    ''',
    dag = dag
)

reformat_equity_yearly = BigQueryOperator(
    task_id = 'reformat_equity_yearly',
    use_legacy_sql = False,
    sql = f'''
    create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_yearly` as
        (SELECT distinct ticker, '2021' as year, year2021 as equity FROM `{PROJECT_ID}.{STAGING_DATASET}.equity_yearly`)
    ''',
    dag = dag
)

reformat_div_yearly = BigQueryOperator(
    task_id = 'reformat_div_yearly',
    use_legacy_sql = False,
    sql = f'''
    create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_yearly` as
        (SELECT distinct ticker, '2021' as year, year2021 as dividends FROM `{PROJECT_ID}.{STAGING_DATASET}.div_yearly`)
    ''',
    dag = dag
)

# join the tables, ensure column types
join_financials_yearly = BigQueryOperator(
    task_id = 'join_financials_yearly',
    use_legacy_sql = False,
    sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.financials_join_yearly` 
        (
            ticker string not null,
            year string not null,
            netincome float64, 
            assets float64, 
            liability float64, 
            equity float64, 
            dividends float64
        )
        as
        select iale.ticker, iale.year, iale.netincome, iale.assets, iale.liability, iale.equity, d.dividends
        from 
        (select ial.ticker, ial.year, ial.netincome, ial.assets, ial.liability, e.equity
        from 
        (select ia.ticker, ia.year, ia.netincome, ia.assets, l.liability 
        from (SELECT i.ticker, i.year, i.netincome, a.assets FROM `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_yearly` i
                left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_yearly` a 
                        on i.ticker = a.ticker and i.year = a.year) as ia 
                left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_yearly` l
                        on ia.ticker = l.ticker and ia.year = l.year) ial
                left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_yearly` e
                        on ial.ticker = e.ticker and ial.year = e.year) iale
                left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_yearly` d
                        on iale.ticker = d.ticker and iale.year = d.year
    ''',
    dag = dag
)

# Add financial ratio calculations
add_financial_ratios_yearly = BigQueryOperator(
    task_id = 'add_financial_ratios_yearly',
    use_legacy_sql = False,
    sql = f'''
            create or replace table `{PROJECT_ID}.{STAGING_DATASET}.financials_with_ratios_yearly` 
            (
                ticker string not null,
                year timestamp not null,
                netincome float64, 
                assets float64, 
                liability float64, 
                equity float64, 
                yearlydividends float64,
                ROA float64,
                ROE float64,
                debttoequity float64,
                networth float64
            )
            as
            select concat(ticker, '.SI') as ticker, parse_timestamp("%Y-%m-%d", concat(year, '-12-31')) as year, netincome, assets, liability, equity, dividends,
            (case when netincome is null then null
            when netincome = 0 then 0
            when assets is null then null
            when assets = 0 then null 
            else netincome/assets end) as ROA, 

            (case when netincome is null then null
            when netincome = 0 then 0
            when equity is null then null
            when equity = 0 then null 
            else netincome/equity end) as ROE,

            (case when liability is null then null
            when liability = 0 then 0
            when equity is null then null
            when equity = 0 then null 
            else liability/equity end) as debttoequity, 

            (case when liability is null then null
            when assets is null then null
            else liability-assets end) as networth,  
            from
            `{PROJECT_ID}.{STAGING_DATASET}.financials_join_yearly`
    ''',
    dag = dag
)


##################
# Choosing Paths #
##################


# Check if netincome exists in GCS, choose path (init or yearly)
def check_netincome_choose(**kwargs):
    netincome = 'netincome_init.parquet'
    if (storage.Blob(bucket = bucket, name = netincome).exists(storage_client)):
        return 'scrape_netincome_yearly'
    else:
        return 'scrape_netincome_init'

# Check if assets exists in GCS, choose path (init or yearly)
def check_assets_choose(**kwargs):
    assets = 'assets_init.parquet'
    if (storage.Blob(bucket = bucket, name = assets).exists(storage_client)):
        return 'scrape_assets_yearly'
    else:
        return 'scrape_assets_init'

# Check if liability exists in GCS, choose path (init or yearly)
def check_liab_choose(**kwargs):
    liability = 'liab_init.parquet'
    if (storage.Blob(bucket = bucket, name = liability).exists(storage_client)):
        return 'scrape_liab_yearly'
    else:
        return 'scrape_liab_init'

# Check if equity exists in GCS, choose path (init or yearly)
def check_equity_choose(**kwargs):
    equity = 'equity_init.parquet'
    if (storage.Blob(bucket = bucket, name = equity).exists(storage_client)):
        return 'scrape_equity_yearly'
    else:
        return 'scrape_equity_init'

# Check if dividends exists in GCS, choose path (init or yearly)
def check_div_choose(**kwargs):
    dividends =  'div_init.parquet'
    if (storage.Blob(bucket = bucket, name = dividends).exists(storage_client)):
        return 'scrape_div_yearly'
    else:
        return 'scrape_div_init'

# Check if inflation exists in GCS, choose path (init or yearly)
def check_inflation_choose(**kwargs):
    inflation =  'inflation_init.parquet'
    if (storage.Blob(bucket = bucket, name = inflation).exists(storage_client)):
        return 'scrape_inflation_yearly'
    else:
        return 'scrape_inflation_init'

check_netincome_choose_path = BranchPythonOperator(
    task_id = 'check_netincome_choose_path',
    python_callable = check_netincome_choose,
    do_xcom_push = False,
    dag = dag
)

check_assets_choose_path = BranchPythonOperator(
    task_id = 'check_assets_choose_path',
    python_callable = check_assets_choose,
    do_xcom_push = False,
    dag = dag
)

check_liab_choose_path = BranchPythonOperator(
    task_id = 'check_liab_choose_path',
    python_callable = check_liab_choose,
    do_xcom_push = False,
    dag = dag
)

check_equity_choose_path = BranchPythonOperator(
    task_id = 'check_equity_choose_path',
    python_callable = check_equity_choose,
    do_xcom_push = False,
    dag = dag
)

check_div_choose_path = BranchPythonOperator(
    task_id = 'check_div_choose_path',
    python_callable = check_div_choose,
    do_xcom_push = False,
    dag = dag
)

check_inflation_choose_path = BranchPythonOperator(
    task_id = 'check_inflation_choose_path',
    python_callable = check_inflation_choose,
    do_xcom_push = False,
    dag = dag
)


## INTO DATAWAREHOUSE
###########
# Loading #
# (INIT)  #
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

create_d_financials = BigQueryOperator(
    task_id = 'create_d_financials',
    use_legacy_sql = False,
    params = {
        'project_id': PROJECT_ID,
        'staging_dataset': STAGING_DATASET,
        'dwh_dataset': DWH_DATASET
    },
    sql = './sql/D_financials.sql',
    dag = dag
)


###########
# Loading #
# YEARLY  #
###########

# not sure, to figure out.
append_F_stock = DummyOperator(
    task_id = 'append_F_stock',
    dag = dag
)

append_D_financials = BigQueryOperator(
    task_id = 'append_D_financials',
    use_legacy_sql = False,
    sql = f'''
        INSERT `{PROJECT_ID}.{DWH_DATASET}.D_financials` 
        SELECT DISTINCT * FROM `{PROJECT_ID}.{STAGING_DATASET}.financials_with_ratios_yearly`
    ''',
    dag = dag
)


############################
# Define Tasks Hierarchy   #
############################
start_pipeline >> [check_netincome_choose_path, check_assets_choose_path, check_liab_choose_path, check_equity_choose_path, check_div_choose_path, check_inflation_choose_path]

check_netincome_choose_path >> [scrape_netincome_init, scrape_netincome_yearly]
check_assets_choose_path >> [scrape_assets_init, scrape_assets_yearly]
check_liab_choose_path >> [scrape_liab_init, scrape_liab_yearly]
check_equity_choose_path >> [scrape_equity_init, scrape_equity_yearly]
check_div_choose_path >> [scrape_div_init, scrape_div_yearly]
check_inflation_choose_path >> [scrape_inflation_init, scrape_inflation_yearly]

scrape_netincome_init >> income_scraping
scrape_assets_init >> assets_scraping
scrape_liab_init >> liab_scraping
scrape_equity_init >> equity_scraping
scrape_div_init >> dividends_scraping
scrape_inflation_init >> inflation_scraping
[income_scraping, assets_scraping, liab_scraping, equity_scraping, dividends_scraping, inflation_scraping] >> financials_cloud_init
financials_cloud_init >> [stage_netincome_init, stage_assets_init, stage_liab_init, stage_equity_init, stage_div_init, stage_inflation_init]
stage_netincome_init >> reformat_netincome_init
stage_assets_init >> reformat_assets_init
stage_liab_init >> reformat_liab_init
stage_equity_init >> reformat_equity_init
stage_div_init >> reformat_div_init
[reformat_netincome_init, reformat_assets_init, reformat_liab_init, reformat_equity_init, reformat_div_init] >> join_financials >> add_financial_ratios >> create_stocks_data >> create_d_financials >> end_init

scrape_netincome_yearly >> income_scraping_yearly
scrape_assets_yearly >> assets_scraping_yearly
scrape_liab_yearly >> liab_scraping_yearly
scrape_equity_yearly >> equity_scraping_yearly
scrape_div_yearly >> dividends_scraping_yearly
scrape_inflation_yearly >> inflation_scraping_yearly
[income_scraping_yearly, assets_scraping_yearly, liab_scraping_yearly, equity_scraping_yearly, dividends_scraping_yearly, inflation_scraping_yearly] >> financials_cloud_yearly
financials_cloud_yearly >> [stage_netincome_yearly, stage_assets_yearly, stage_liab_yearly, stage_equity_yearly, stage_div_yearly, stage_inflation_yearly]
stage_netincome_yearly >> reformat_netincome_yearly
stage_assets_yearly >> reformat_assets_yearly
stage_liab_yearly >> reformat_liab_yearly
stage_equity_yearly >> reformat_equity_yearly
stage_div_yearly >> reformat_div_yearly
[reformat_netincome_yearly, reformat_assets_yearly, reformat_liab_yearly, reformat_equity_yearly, reformat_div_yearly] >> join_financials_yearly >> add_financial_ratios_yearly >> append_D_financials >> append_F_stock >> end_yearly