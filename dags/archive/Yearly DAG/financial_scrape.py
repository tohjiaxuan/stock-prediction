from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
import cchardet
import pandas as pd
import requests
from bs4 import BeautifulSoup

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}


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
    schedule_interval = "@daily",
    catchup = False
)


############################
# Define Python Functions  #
############################


# tickers_df wil be global
tickers_df = pd.read_csv('/home/airflow/airflow/dags/SGX.csv')


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
            print('this is com_final_coms')
            print(com_final_cols)
            #new_df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
            df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
    
    return df

# Function for (1) scraping net income and (2) storing into psql table
def income_scraping_data(**kwargs):
    table_header = ['Ticker', 'Net Income', 2021, 2020, 2019, 2018, 2017]
    df = pd.DataFrame(columns=table_header)
    wsj_start = 'https://www.wsj.com/market-data/quotes/SG/'
    wsj_end = '/financials/annual/income-statement'
    no_data = []
    #for i in tickers_df['Symbol']:
    for i in ['1A1','1A4', 'D05']:
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
    df[[2021, 2020, 2019, 2018, 2017]].astype(float)
    df_list = df.values.tolist()
    print(df_list)
    for result in df_list:
        print('this is result')
        print(result)
        print('this is query')
        query = f'''
        INSERT INTO netincome (ticker, netincome, year2021, year2020, year2019, year2018)
        VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}');
        '''
        print(query)
        execute_query_with_hook(query)
    #return df_list


# Function for (1) scraping assets and (2) storing into psql table
def assets_scraping_data(**kwargs):
    table_header_assets = ['Ticker', 'Total Assets', 2021, 2020, 2019, 2018, 2017]
    df_assets = pd.DataFrame(columns=table_header_assets)
    wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
    wsj_end_bs = '/financials/annual/balance-sheet'
    no_data_assets = []
    #for i in tickers_df['Symbol']:
    for i in ['1A1','1A4', 'D05']:
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

    df_assets[[2021, 2020, 2019, 2018, 2017]].astype(float)
    df_assets_list = df_assets.values.tolist()
    print(df_assets_list)
    for result in df_assets_list:
        print('this is result')
        print(result)
        print('this is query')
        query = f'''
        INSERT INTO assets (ticker, assets, year2021, year2020, year2019, year2018)
        VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}');
        '''
        print(query)
        execute_query_with_hook(query)


# Function for (1) scraping liabilities and (2) storing into psql table
def liab_scraping_data(**kwargs):
    table_header_liab = ['Ticker', 'Total Liabilities', 2021, 2020, 2019, 2018, 2017]
    df_liab = pd.DataFrame(columns=table_header_liab)
    wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
    wsj_end_bs = '/financials/annual/balance-sheet'
    no_data_liab = []
    #for i in tickers_df['Symbol']:
    for i in ['1A1','1A4', 'D05']:
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
    df_liab[[2021, 2020, 2019, 2018, 2017]].astype(float)
    df_liab_list = df_liab.values.tolist()
    print(df_liab_list)
    for result in df_liab_list:
        print('this is result')
        print(result)
        print('this is query')
        query = f'''
        INSERT INTO liability (ticker, liability, year2021, year2020, year2019, year2018)
        VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}');
        '''
        print(query)
        execute_query_with_hook(query)

# Function for (1) scraping equity and (2) storing into psql table
def equity_scraping_data(**kwargs):
    table_header_eq = ['Ticker', "Total Shareholders' Equity", 2021, 2020, 2019, 2018, 2017]
    df_eq = pd.DataFrame(columns=table_header_eq)
    wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
    wsj_end_bs = '/financials/annual/balance-sheet'
    no_data_eq = []
    #for i in tickers_df['Symbol']:
    for i in ['1A1','1A4', 'D05']:
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
    df_eq[[2021, 2020, 2019, 2018, 2017]].astype(float)
    df_eq_list = df_eq.values.tolist()
    print(df_eq_list)
    for result in df_eq_list:
        print('this is result')
        print(result)
        print('this is query')
        query = f'''
        INSERT INTO equity (ticker, equity, year2021, year2020, year2019, year2018)
        VALUES ('{result[0]}', '{result[1].replace("s'", 's')}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}');
        '''
        print(query)
        execute_query_with_hook(query)

# Function for (1) scraping dividends and (2) storing into psql table
def dividends_scraping_data(**kwargs):
    table_header_div = ['Ticker', "Cash Dividends Paid - Total", 2021, 2020, 2019, 2018, 2017]
    df_div = pd.DataFrame(columns=table_header_div)
    wsj_start_cf = 'https://www.wsj.com/market-data/quotes/SG/'
    wsj_end_cf = '/financials/annual/cash-flow'
    no_data_div = []
    for i in ['1A1','1A4', 'D05']:
    #for i in tickers_df['Symbol']:
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
    df_div[[2021, 2020, 2019, 2018, 2017]].astype(float)
    df_div_list = df_div.values.tolist()
    print(df_div_list)
    for result in df_div_list:
        print('this is result')
        print(result)
        print('this is query')
        query = f'''
        INSERT INTO dividends (ticker, dividends, year2021, year2020, year2019, year2018)
        VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}');
        '''
        print(query)
        execute_query_with_hook(query)

############################
# Define Airflow Operators #
############################

# Create table to store net income (netincome)
create_table_income = PostgresOperator (
    task_id = 'create_table_income',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS netincome (
        ticker TEXT NOT NULL PRIMARY KEY,
        netincome TEXT NOT NULL,
        year2021 REAL,
        year2020 REAL,
        year2019 REAL,
        year2018 REAL
        );
        '''
    )


# Create table to store assets (assets)
create_table_assets = PostgresOperator (
    task_id = 'create_table_assets',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS assets (
        ticker TEXT NOT NULL PRIMARY KEY,
        assets TEXT NOT NULL,
        year2021 REAL,
        year2020 REAL,
        year2019 REAL,
        year2018 REAL
        );
        '''
    )

# Create table to store liabilities (liability)
create_table_liab = PostgresOperator (
    task_id = 'create_table_liab',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS liability (
        ticker TEXT NOT NULL PRIMARY KEY,
        liability TEXT NOT NULL,
        year2021 REAL,
        year2020 REAL,
        year2019 REAL,
        year2018 REAL
        );
        '''
    )

# Create table to store equity (equity)
create_table_equity = PostgresOperator (
    task_id = 'create_table_equity',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS equity (
        ticker TEXT NOT NULL PRIMARY KEY,
        equity TEXT NOT NULL,
        year2021 REAL,
        year2020 REAL,
        year2019 REAL,
        year2018 REAL
        );
        '''
    )

# Create table to store dividends (dividends)
create_table_dividends = PostgresOperator (
    task_id = 'create_table_dividends',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS dividends (
        ticker TEXT NOT NULL PRIMARY KEY,
        dividends TEXT NOT NULL,
        year2021 REAL,
        year2020 REAL,
        year2019 REAL,
        year2018 REAL
        );
        '''
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



############################
# Define Tasks Hierarchy   #
############################

create_table_income >> create_table_assets >> create_table_liab >> create_table_equity >> create_table_dividends >> [income_scraping, assets_scraping, equity_scraping, liab_scraping, dividends_scraping]