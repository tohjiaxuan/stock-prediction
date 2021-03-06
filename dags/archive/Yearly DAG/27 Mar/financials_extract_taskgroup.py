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
from airflow.utils.task_group import TaskGroup
from google.cloud.exceptions import NotFound
from google.cloud import bigquery


headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'


def build_financials_extract_taskgroup(dag: DAG) -> TaskGroup:
    financials_extract_taskgroup = TaskGroup(group_id = 'financials_extract_tg')

    # LOAD TICKERS
    # tickers_df wil be global
    tickers_df = pd.read_csv('/home/airflow/airflow/dags/sti.csv')

    # GLOBAL HELPER FUNCTIONS
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

    # Function for (1) scraping net income 
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
        


    # Function for (1) scraping assets 
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


    # Function for (1) scraping liabilities 
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

    # Function for (1) scraping equity 
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

    # Function for (1) scraping dividends 
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

    # Function for (1) scraping net income 
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


    # Function for (1) scraping assets 
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


    # Function for (1) scraping liabilities
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

    # Function for (1) scraping equity
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

    # Function for (1) scraping dividends 
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

    prep_gcs = BashOperator(
        task_id="prep_gcs",
        bash_command="echo prep_gcs",
        trigger_rule="all_done",
        dag=dag
    )


    ############################
    # NEW!                     #
    ############################


    def if_d_financials_exists(**kwargs):
        client_bq = bigquery.Client(project=PROJECT_ID)
        table_ref = "stockprediction-344203.stock_prediction_datawarehouse.D_FINANCIALS"
        try:
            table = client_bq.get_table(table_ref)
            if table:
                return True
            
        except NotFound as error:
            return False

    def if_d_inflation_exists(**kwargs):
        client_bq = bigquery.Client(project=PROJECT_ID)
        table_ref = "stockprediction-344203.stock_prediction_datawarehouse.D_INFLATION"
        try:
            table = client_bq.get_table(table_ref)
            if table:
                return True
            
        except NotFound as error:
            print('testing')
            return False

    def scrape_netincome():
        check_dwh = if_d_financials_exists()
        if check_dwh:
            netincome_df = income_scraping_data_yearly()
        else: 
            netincome_df = income_scraping_data()
        return netincome_df
    
    def scrape_assets():
        check_dwh = if_d_financials_exists()
        if check_dwh:
            assets_df = assets_scraping_data_yearly()
        else: 
            assets_df = assets_scraping_data()
        return assets_df

    def scrape_liab():
        check_dwh = if_d_financials_exists()
        if check_dwh:
            liab_df = liab_scraping_data_yearly()
        else: 
            liab_df = liab_scraping_data()
        return liab_df

    def scrape_equity():
        check_dwh = if_d_financials_exists()
        if check_dwh:
            equity_df = equity_scraping_data_yearly()
        else: 
            equity_df = equity_scraping_data()
        return equity_df

    def scrape_dividends():
        check_dwh = if_d_financials_exists()
        if check_dwh:
            div_df = dividends_scraping_data_yearly()
        else: 
            div_df = dividends_scraping_data()
        return div_df
    
    def scrape_inflation():
        check_dwh = if_d_inflation_exists()
        if check_dwh:
            inflation_df = inflation_scraping_data_yearly()
        else: 
            inflation_df = inflation_scraping_data()
        return inflation_df

    #################################
    # Airflow Operators             #
    #################################

    # Scraping annual income 
    income_scraping = PythonOperator(
        task_id = 'income_scraping',
        python_callable = scrape_netincome,
        dag = dag
    )

    # Scraping annual assets
    assets_scraping = PythonOperator(
        task_id = 'assets_scraping',
        python_callable = scrape_assets,
        dag = dag
    )

    # Scraping annual liab
    liab_scraping = PythonOperator(
        task_id = 'liab_scraping',
        python_callable = scrape_liab,
        dag = dag
    )

    # Scraping annual equity
    equity_scraping = PythonOperator(
        task_id = 'equity_scraping',
        python_callable = scrape_equity,
        dag = dag
    )

    # Scraping annual dividends
    dividends_scraping = PythonOperator(
        task_id = 'dividends_scraping',
        python_callable = scrape_dividends,
        dag = dag
    )

    # Scraping annual inflation
    inflation_scraping = PythonOperator(
        task_id = 'inflation_scraping',
        python_callable = scrape_inflation,
        dag = dag
    )

    
    
    # TASK DEPENDENCIES

    start_pipeline >> [income_scraping, assets_scraping, liab_scraping, equity_scraping, dividends_scraping, inflation_scraping]
    [income_scraping, assets_scraping, liab_scraping, equity_scraping, dividends_scraping, inflation_scraping] >> prep_gcs

    
    return financials_extract_taskgroup