from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
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
import urllib.request


headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

indicator = []

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


# Helper function for scraping of inflation rate
def get_inflation(latest_year, val_interest):
    
    # Intialise variables needed in function
    counter = 0
    curr_year = str(latest_year) # Ensure variable type is string
    year = []
    inflation = []
    
    for val in val_interest:
        if curr_year in val:
            year.append(curr_year)
            curr_year = str(int(curr_year) - 1)
            counter += 1
        elif counter == 13:
            value = val.get_text()
            if not value:
                inflation.append(None)
            else:
                inflation.append(float(value[:-1]))
            counter = 0
        else:
            counter += 1
    
    # Return df
    df = pd.DataFrame(year, columns = ['Year'])
    df['Inflation (%)'] = inflation
    
    return df

# Function for (1) scraping net income and (2) storing into psql table
def income_scraping_data(**kwargs):
    table_header = ['Ticker', 'Net Income', 2021, 2020, 2019, 2018, 2017]
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


# Function for (1) scraping inflation rate and (2) storing into psql table 
def inflation_scraping_data(**kwargs):
    url = 'https://www.rateinflation.com/inflation-rate/singapore-inflation-rate/'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "lxml")
    wants = soup.find_all("td")
    df = get_inflation('2022', wants)
    df_list = df.values.tolist()
    for result in df_list:
        print('this is result')
        print(result)
        print('this is query')
        query = f'''
        INSERT INTO inflation (year, inflationrate)
        VALUES ('{result[0]}', '{result[1]}');
        '''
        print(query)
        execute_query_with_hook(query)

def exchange_rate_scraping_data(**kwargs):
    url = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=95932927-c8bc-4e7a-b484-68a66a24edfe&limit=1000&sort=end_of_day%20desc'
    req = urllib.request.Request(url,None,headers)
    response = urllib.request.urlopen(req)
    data_new = response.read() 
    new = json.loads(data_new.decode())
    er_batch_1 = new['result']['records']

    url = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=95932927-c8bc-4e7a-b484-68a66a24edfe&limit=100&between[end_of_day]=2018-01-01,2018-03-19'
    req = urllib.request.Request(url,None,headers)
    response = urllib.request.urlopen(req)
    data_new = response.read() 
    new = json.loads(data_new.decode())
    er_batch_2 = new['result']['records']

    er_init_batch = er_batch_1 + er_batch_2

    er_df = pd.DataFrame.from_dict(er_init_batch)
    er_df_list = er_df.values.tolist()
    for result in er_df_list:

        print('this is result')
        print(result)
        print('this is query')
        query = f'''
        INSERT INTO exchangerate (end_of_day, preliminary, eur_sgd, gbp_sgd, usd_sgd, aud_sgd,
       cad_sgd, cny_sgd_100, hkd_sgd_100, inr_sgd_100, idr_sgd_100,
       jpy_sgd_100, krw_sgd_100, myr_sgd_100, twd_sgd_100, nzd_sgd,
       php_sgd_100, qar_sgd_100, sar_sgd_100, chf_sgd, thb_sgd_100,
       aed_sgd_100, vnd_sgd_100, timestamp)
        VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}', '{result[7]}', '{result[8]}', '{result[9]}', '{result[10]}', '{result[11]}', '{result[12]}', '{result[13]}', '{result[14]}', '{result[15]}', '{result[16]}', '{result[17]}', '{result[18]}', '{result[19]}', '{result[20]}', '{result[21]}', '{result[22]}', '{result[23]}');
        '''
        print(query)
        execute_query_with_hook(query)

def interest_rate_scraping_data(**kwargs):
    url = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=1000&sort=end_of_day%20desc'
    req = urllib.request.Request(url,None,headers)
    response = urllib.request.urlopen(req)
    data = response.read()
    raw_batch1 = json.loads(data.decode())
    batch1 = raw_batch1['result']['records']

    url = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=1000&between[end_of_day]=2018-01-01,2018-09-16'
    req = urllib.request.Request(url,None,headers)
    response = urllib.request.urlopen(req)
    data = response.read()
    raw_batch2 = json.loads(data.decode())
    batch2 = raw_batch2['result']['records']

    ir_init_batch = batch1 + batch2

    ir_df = pd.DataFrame.from_dict(ir_init_batch)
    ir_df_list = ir_df.values.tolist()
    for result in ir_df_list:

        print('this is result')
        print(result)
        print('this is query')
        query = f'''
        INSERT INTO interestrate (aggregate_volume, calculation_method, commercial_bills_3m,
       comp_sora_1m, comp_sora_3m, comp_sora_6m, end_of_day,
       highest_transaction, interbank_12m, interbank_1m, interbank_1w,
       interbank_2m, interbank_3m, interbank_6m, interbank_overnight,
       lowest_transaction, on_rmb_facility_rate, preliminary,
       published_date, sgs_repo_overnight_rate, sor_average, sora,
       sora_index, standing_facility_borrow, standing_facility_deposit,
       usd_sibor_3m, timestamp)
        VALUES ('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}', '{result[7]}', '{result[8]}', '{result[9]}', '{result[10]}', '{result[11]}', '{result[12]}', '{result[13]}', '{result[14]}', '{result[15]}', '{result[16]}', '{result[17]}', '{result[18]}', '{result[19]}', '{result[20]}', '{result[21]}', '{result[22]}', '{result[23]}', '{result[24]}', '{result[25]}', '{result[26]}');
        '''
        print(query)
        execute_query_with_hook(query)

# separate dags into 2 paths (Initialisation + Finish Initialisation - yearly)
def choose_path(**kwargs):
    postgres = PostgresHook(postgres_conn_id="postgres_local")
    conn = postgres.get_conn()
    cursor = conn.cursor()
    query = cursor.execute("SELECT EXISTS (SELECT * FROM pg_tables WHERE  schemaname = 'public' AND  tablename  = 'netincome');")
    result = cursor.fetchall()
    print(result)
    if (True, ) in result:
        return 'finish_task'
    else:
        return 'create_table_income'

# filler (for testing dag paths)
def finish_init(**kwargs):
    indicator.append('Initialisation Done')
    print(indicator)
    return indicator

############################
# Define Airflow Operators #
############################


# Create table to store inflation rate (inflation)
create_table_inflation = PostgresOperator (
    task_id = 'create_table_inflation',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS inflation (
        year TEXT NOT NULL PRIMARY KEY,
        inflationrate REAL
        );
        '''
    )

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

# Create table to store ER (exchangerate)
create_table_exchangerate = PostgresOperator (
    task_id = 'create_table_exchangerate',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS exchangerate (
        end_of_day TEXT NOT NULL PRIMARY KEY, 
        preliminary REAL, 
        eur_sgd REAL, 
        gbp_sgd REAL, 
        usd_sgd REAL, 
        aud_sgd REAL,
        cad_sgd REAL, 
        cny_sgd_100 REAL, 
        hkd_sgd_100 REAL, 
        inr_sgd_100 REAL, 
        idr_sgd_100 REAL,
        jpy_sgd_100 REAL, 
        krw_sgd_100 REAL, 
        myr_sgd_100 REAL, 
        twd_sgd_100 REAL, 
        nzd_sgd REAL,
        php_sgd_100 REAL, 
        qar_sgd_100 REAL, 
        sar_sgd_100 REAL, 
        chf_sgd REAL, 
        thb_sgd_100 REAL,
        aed_sgd_100 REAL, 
        vnd_sgd_100 REAL, 
        timestamp REAL
        );
        '''
    )

# Create table to store IR (interestrate)
create_table_interestrate = PostgresOperator (
    task_id = 'create_table_interestrate',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS interestrate (
        aggregate_volume TEXT, 
        calculation_method TEXT, 
        commercial_bills_3m TEXT,
       comp_sora_1m TEXT, 
       comp_sora_3m TEXT, 
       comp_sora_6m TEXT, 
       end_of_day TEXT,
       highest_transaction TEXT, 
       interbank_12m TEXT, 
       interbank_1m TEXT, 
       interbank_1w TEXT,
       interbank_2m TEXT, 
       interbank_3m TEXT, 
       interbank_6m TEXT, 
       interbank_overnight TEXT,
       lowest_transaction TEXT, 
       on_rmb_facility_rate TEXT, 
       preliminary TEXT,
       published_date TEXT, 
       sgs_repo_overnight_rate TEXT, 
       sor_average TEXT, 
       sora TEXT,
       sora_index TEXT, 
       standing_facility_borrow TEXT, 
       standing_facility_deposit TEXT,
       usd_sibor_3m TEXT, 
       timestamp TEXT
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

# Scraping annual inflation rates
inflation_scraping = PythonOperator(
    task_id = 'inflation_scraping_data',
    python_callable = inflation_scraping_data,
    dag = dag
)

# Scraping annual exchange rates
exchange_rate_scraping = PythonOperator(
    task_id = 'exchange_rate_scraping_data',
    python_callable = exchange_rate_scraping_data,
    dag = dag
)

# Scraping annual interest rates
interest_rate_scraping = PythonOperator(
    task_id = 'interest_rate_scraping_data',
    python_callable = interest_rate_scraping_data,
    dag = dag
)



# filler (for testing dag paths)
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

# choosing the best path
choose_best_path = BranchPythonOperator(
    task_id = 'choose_path',
    python_callable = choose_path,
    dag = dag
)

# staging transformation: reformat financials tables (equity, dividends, equity, liability, assets):
reformat_financials = PostgresOperator (
    task_id = 'reformat_financials',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        select distinct * into reformat_netincome
        from ((select ticker, '2021', year2021 from netincome) 
                union all 
              (select ticker, '2020', year2020 from netincome) 
                union all 
              (select ticker, '2019', year2019 from netincome) 
                union all 
              (select ticker, '2018', year2018 from netincome)) as temp;
        alter table reformat_netincome rename "?column?" to year;
        alter table reformat_netincome rename year2021 to netincome;
        alter table reformat_netincome alter column year type smallint using year::smallint, 
                                       alter column ticker type text, 
                                       alter column netincome type real;

        select distinct * into reformat_assets
        from ((select ticker, '2021', year2021 from assets) 
                union all 
              (select ticker, '2020', year2020 from assets) 
                union all 
              (select ticker, '2019', year2019 from assets) 
                union all 
              (select ticker, '2018', year2018 from assets)) as temp;
        alter table reformat_assets rename "?column?" to year;
        alter table reformat_assets rename year2021 to assets;
        alter table reformat_assets alter column year type smallint using year::smallint, 
                                       alter column ticker type text, 
                                       alter column assets type real;

        select distinct * into reformat_liability
        from ((select ticker, '2021', year2021 from liability) 
                union all 
              (select ticker, '2020', year2020 from liability) 
                union all 
              (select ticker, '2019', year2019 from liability) 
                union all 
              (select ticker, '2018', year2018 from liability)) as temp;
        alter table reformat_liability rename "?column?" to year;
        alter table reformat_liability rename year2021 to liabilities;
        alter table reformat_liability alter column year type smallint using year::smallint, 
                                       alter column ticker type text, 
                                       alter column liabilities type real;

        select distinct * into reformat_dividends
        from ((select ticker, '2021', year2021 from dividends) 
                union all 
              (select ticker, '2020', year2020 from dividends) 
                union all 
              (select ticker, '2019', year2019 from dividends) 
                union all 
              (select ticker, '2018', year2018 from dividends)) as temp;
        alter table reformat_dividends rename "?column?" to year;
        alter table reformat_dividends rename year2021 to dividends;
        alter table reformat_dividends alter column year type smallint using year::smallint, 
                                       alter column ticker type text, 
                                       alter column dividends type real;

        select distinct * into reformat_equity
        from ((select ticker, '2021', year2021 from equity) 
                union all 
              (select ticker, '2020', year2020 from equity) 
                union all 
              (select ticker, '2019', year2019 from equity) 
                union all 
              (select ticker, '2018', year2018 from equity)) as temp;
        alter table reformat_equity rename "?column?" to year;
        alter table reformat_equity rename year2021 to shareholders_equity;
        alter table reformat_equity alter column year type smallint using year::smallint, 
                                       alter column ticker type text, 
                                       alter column shareholders_equity type real;
        '''
    )

# staging transformation: join financials tables (prep for calculation of financial ratios)
join_financials = PostgresOperator (
    task_id = 'join_financials',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
            select niale.ticker, niale.year, niale.netincome, niale.assets, niale.liabilities, niale.shareholders_equity, d.dividends into temp_join from 
                    ((select nial.ticker, nial.year, nial.netincome, nial.assets, nial.liabilities, e.shareholders_equity from
                    (select nia.ticker, nia.year, nia.netincome, nia.assets, l.liabilities from 
                    (select ni.ticker, ni.year, ni.netincome, a.assets from reformat_netincome ni 
                    inner join reformat_assets a on ni.ticker = a.ticker and ni.year = a.year) nia 
                        inner join reformat_liability l on nia.ticker = l.ticker and nia.year = l.year) nial
                            inner join reformat_equity e on nial.ticker = e.ticker and nial.year = e.year) niale
                                inner join reformat_dividends d on niale.ticker = d.ticker and niale.year = d.year);
    
        
        '''
    )

# staging transformation: calculating financial ratios
financial_ratios = PostgresOperator (
    task_id = 'financial_ratios',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        
        ALTER TABLE TEMP_JOIN ADD COLUMN ROA REAL GENERATED ALWAYS AS
            (CASE WHEN NETINCOME = 0 THEN 0
                  WHEN ASSETS = 0 THEN 0
                  ELSE NETINCOME/ASSETS END) STORED;

        ALTER TABLE TEMP_JOIN ADD COLUMN ROE REAL GENERATED ALWAYS AS
            (CASE WHEN NETINCOME = 0 THEN 0
                  WHEN SHAREHOLDERS_EQUITY = 0 THEN 0
                  ELSE NETINCOME/SHAREHOLDERS_EQUITY END) STORED;

        ALTER TABLE TEMP_JOIN ADD COLUMN DEBT_TO_EQUITY REAL GENERATED ALWAYS AS
            (CASE WHEN LIABILITIES = 0 THEN 0
                  WHEN SHAREHOLDERS_EQUITY = 0 THEN 0
                  ELSE LIABILITIES/SHAREHOLDERS_EQUITY END) STORED;

        ALTER TABLE TEMP_JOIN ADD COLUMN NETWORTH REAL GENERATED ALWAYS AS
            (ASSETS - LIABILITIES) stored;
    
        
        '''
    )
############################
# Define Tasks Hierarchy   #
############################

choose_best_path >> create_table_income >> create_table_assets >> create_table_liab >> create_table_equity >> create_table_dividends >> create_table_inflation >> create_table_exchangerate >> create_table_interestrate >> income_scraping >> assets_scraping >> equity_scraping >> liab_scraping >> dividends_scraping >> inflation_scraping >> exchange_rate_scraping >> interest_rate_scraping >> reformat_financials >> join_financials >> financial_ratios >> finish_start_init 
choose_best_path >> finish_start