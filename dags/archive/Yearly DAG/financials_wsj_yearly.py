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
# Yearly                        #
#################################

# Function for (1) scraping net income and (2) storing into gcs
def income_scraping_data_yearly(**kwargs):
    # to ensure that date is dynamic in a sense that the column name of the dataframe will be the current year, 
    # this line should be used. 
    curr_year = str(date.today().year)
    # however, for demo purposes to show proof of code functionality, 
    # we are going to mimic the current year as 2021 to show data, hence we minus 1 to make curr_year as 2021: 
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
    # to ensure that date is dynamic in a sense that the column name of the dataframe will be the current year, 
    # this line should be used. 
    curr_year = str(date.today().year)
    # however, for demo purposes to show proof of code functionality, 
    # we are going to mimic the current year as 2021 to show data, hence we minus 1 to make curr_year as 2021: 
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
    # to ensure that date is dynamic in a sense that the column name of the dataframe will be the current year, 
    # this line should be used. 
    curr_year = str(date.today().year)
    # however, for demo purposes to show proof of code functionality, 
    # we are going to mimic the current year as 2021 to show data, hence we minus 1 to make curr_year as 2021: 
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
    # to ensure that date is dynamic in a sense that the column name of the dataframe will be the current year, 
    # this line should be used. 
    curr_year = str(date.today().year)
    # however, for demo purposes to show proof of code functionality, 
    # we are going to mimic the current year as 2021 to show data, hence we minus 1 to make curr_year as 2021: 
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
    # to ensure that date is dynamic in a sense that the column name of the dataframe will be the current year, 
    # this line should be used. 
    curr_year = str(date.today().year)
    # however, for demo purposes to show proof of code functionality, 
    # we are going to mimic the current year as 2021 to show data, hence we minus 1 to make curr_year as 2021: 
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
            # Here, we are simulating with 2021 data to show proof of code functionality.
            # That is why we add a -1 to the current year (i.e. 2022) // 2022 - 1 = 2021
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

scrape_init = DummyOperator(
    task_id = 'scrape_init',
    dag = dag
)

intermediate = DummyOperator(
    task_id = 'intermediate',
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
                year string not null,
                netincome float64, 
                assets float64, 
                liability float64, 
                equity float64, 
                dividends float64,
                ROA float64,
                ROE float64,
                debttoequity float64,
                networth float64
            )
            as
            select ticker, year, netincome, assets, liability, equity, dividends,
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

############################
# Define Tasks Hierarchy   #
############################



start_pipeline >> scrape_init >> [income_scraping_yearly, assets_scraping_yearly, liab_scraping_yearly, equity_scraping_yearly, dividends_scraping_yearly, inflation_scraping_yearly] 
[income_scraping_yearly, assets_scraping_yearly, liab_scraping_yearly, equity_scraping_yearly, dividends_scraping_yearly, inflation_scraping_yearly]  >> financials_cloud_yearly
financials_cloud_yearly >> [stage_netincome_yearly, stage_assets_yearly, stage_liab_yearly, stage_equity_yearly, stage_div_yearly, stage_inflation_yearly] >> intermediate
intermediate >> [reformat_netincome_yearly, reformat_assets_yearly, reformat_liab_yearly, reformat_equity_yearly, reformat_div_yearly] >> join_financials_yearly >> add_financial_ratios_yearly
