from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
from bs4 import BeautifulSoup
from datetime import datetime, date
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

import cchardet
import json
import os
import pandas as pd
import requests
import urllib.request


headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'


def build_financials_extract_taskgroup(dag: DAG) -> TaskGroup:
    financials_extract_taskgroup = TaskGroup(group_id = 'financials_extract_tg')

    # Load tickers stored as a separate CSV file.
    tickers_df = pd.read_csv('/home/airflow/airflow/dags/sti.csv')


    def cond(x):
        """ Helper function to scrape data

        Returns
        -------
        boolean 
        
        """
        if x.startswith("hide"):
            return False
        else:
            return True

    
    def replace_dash(list_needed):
        """ Helper function to scrape data
        Parameters
        ----------
        list 

        Returns
        -------
        list 
        
        """
        return ['0' if item == '-' else item for item in list_needed]

    #################################
    # Helper Functions for Scrape   #
    # (Initialisation - Historical) #
    #################################

    # Scraping the historical data (i.e. from 2017 onwards)

    def table_content_income(ticker, table, df, col_names):
        """ Helper function to scrape net income 
        Scrapes net income content in table from Wall Street Journal website

        Parameters
        ----------
        ticker, table, dataframe, column names 

        Returns
        -------
        dataframe 
        
        """
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

    def table_content_assets(ticker, table, df, col_names):
        """ Helper function to scrape assets 
        Scrapes assets content in table from Wall Street Journal website

        Parameters
        ----------
        ticker, table, dataframe, column names 

        Returns
        -------
        dataframe 
        
        """
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

    def table_content_liab(ticker, table, df, col_names):
        """ Helper function to scrape liabilities 
        Scrapes liabilities content in table from Wall Street Journal website

        Parameters
        ----------
        ticker, table, dataframe, column names 

        Returns
        -------
        dataframe 
        
        """
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

    def table_content_equity(ticker, table, df, col_names):
        """ Helper function to scrape equity 
        Scrapes equity content in table from Wall Street Journal website

        Parameters
        ----------
        ticker, table, dataframe, column names 

        Returns
        -------
        dataframe 
        
        """
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

    def table_content_div(ticker, table, df, col_names):
        """ Helper function to scrape dividends 
        Scrapes dividends content in table from Wall Street Journal website

        Parameters
        ----------
        ticker, table, dataframe, column names 

        Returns
        -------
        dataframe 
        
        """
        table_body = table.find('tbody')
        rows = table_body.find_all('tr', {'class': cond})
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
            filter_cols = list(filter(None, cols))
            if not filter_cols:
                continue
            if filter_cols[0] == "Cash Dividends Paid - Total":
                final_cols = replace_dash(filter_cols)
                com_final_cols = [ticker] + final_cols
                df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
        
        return df
    

    #################################
    # Helper Functions for Scrape   #
    # (Yearly)                      #
    #################################

    # Scraping the up-to-date data (yearly)

    def table_content_income_yearly(ticker, table, df, col_names):
        """ Helper function to scrape net income (yearly)
        Scrapes net income content in table from Wall Street Journal website

        Parameters
        ----------
        ticker, table, dataframe, column names 

        Returns
        -------
        dataframe 
        
        """
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

    def table_content_assets_yearly(ticker, table, df, col_names):
        """ Helper function to scrape assets (yearly)
        Scrapes assets content in table from Wall Street Journal website

        Parameters
        ----------
        ticker, table, dataframe, column names 

        Returns
        -------
        dataframe 
        
        """
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

    def table_content_liab_yearly(ticker, table, df, col_names):
        """ Helper function to scrape liabilities (yearly)
        Scrapes liabilities content in table from Wall Street Journal website

        Parameters
        ----------
        ticker, table, dataframe, column names 

        Returns
        -------
        dataframe 
        
        """
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

    def table_content_equity_yearly(ticker, table, df, col_names):
        """ Helper function to scrape equity (yearly)
        Scrapes equity content in table from Wall Street Journal website

        Parameters
        ----------
        ticker, table, dataframe, column names 

        Returns
        -------
        dataframe 
        
        """
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

    def table_content_div_yearly(ticker, table, df, col_names):
        """ Helper function to scrape dividends (yearly)
        Scrapes dividends content in table from Wall Street Journal website

        Parameters
        ----------
        ticker, table, dataframe, column names 

        Returns
        -------
        dataframe 
        
        """
        table_body = table.find('tbody')
        rows = table_body.find_all('tr', {'class': cond})
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip().replace(',', '').replace('(', '-').replace(')', '') for ele in cols]
            filter_cols = list(filter(None, cols))
            if not filter_cols:
                continue
            if filter_cols[0] == "Cash Dividends Paid - Total":
                intm_cols = replace_dash(filter_cols)
                final_cols = intm_cols[0:2]
                com_final_cols = [ticker] + final_cols
                df = df.append(pd.DataFrame([com_final_cols], columns=col_names),ignore_index=True)
        
        return df

    #################################
    # Functions for Scrape          #
    # (Initialisation - Historical) #
    #################################

    def income_scraping_data(**kwargs):
        """ Scrapes net income data from Wall Street Journal website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        table_header = ['Ticker', 'Net Income', 'Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']
        df = pd.DataFrame(columns=table_header)
        wsj_start = 'https://www.wsj.com/market-data/quotes/SG/'
        wsj_end = '/financials/annual/income-statement'
        no_data = []
        for i in tickers_df['Symbol']:
            try: 
                wsj_url = wsj_start + i + wsj_end
                response = requests.get(wsj_url, headers=headers)
                soup = BeautifulSoup(response.text, "lxml")
                wants = soup.find_all("tr")
                table = soup.find('table', attrs={'class':'cr_dataTable'})
                df = table_content_income(i, table, df, table_header)
                
            except AttributeError as e:
                no_data.append(i)

        
        df[['Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']] = df[['Year2021','Year2020', 'Year2019', 'Year2018', 'Year2017']].astype(float)
        
        return df
        
    def assets_scraping_data(**kwargs):
        """ Scrapes assets data from Wall Street Journal website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        table_header_assets = ['Ticker', 'Total Assets', 'Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']
        df_assets = pd.DataFrame(columns=table_header_assets)
        wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
        wsj_end_bs = '/financials/annual/balance-sheet'
        no_data_assets = []
        for i in tickers_df['Symbol']:
            try: 
                wsj_url = wsj_start_bs + i + wsj_end_bs
                response = requests.get(wsj_url, headers=headers)
                soup = BeautifulSoup(response.text, "lxml")
                wants = soup.find_all("tr")
                table = soup.find('table', attrs={'class':'cr_dataTable'})
                df_assets = table_content_assets(i, table, df_assets, table_header_assets)
                
            except AttributeError as e:
                no_data_assets.append(i)

        df_assets[['Year2021','Year2020', 'Year2019', 'Year2018', 'Year2017']] = df_assets[['Year2021','Year2020', 'Year2019', 'Year2018', 'Year2017']].astype(float)
        
        return df_assets

    def liab_scraping_data(**kwargs):
        """ Scrapes liabilities data from Wall Street Journal website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        table_header_liab = ['Ticker', 'Total Liabilities', 'Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']
        df_liab = pd.DataFrame(columns=table_header_liab)
        wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
        wsj_end_bs = '/financials/annual/balance-sheet'
        no_data_liab = []
        for i in tickers_df['Symbol']:
            try: 
                wsj_url = wsj_start_bs + i + wsj_end_bs
                response = requests.get(wsj_url, headers=headers)
                soup = BeautifulSoup(response.text, "lxml")
                wants = soup.find('div', class_='collapsed')
                table = wants.find('table', attrs={'class':'cr_dataTable'})
                df_liab = table_content_liab(i, table, df_liab, table_header_liab)
                
            except AttributeError as e:
                no_data_liab.append(i)
   
        df_liab[['Year2021','Year2020', 'Year2019', 'Year2018', 'Year2017']] = df_liab[['Year2021','Year2020', 'Year2019', 'Year2018', 'Year2017']].astype(float) 
        
        return df_liab

    def equity_scraping_data(**kwargs):
        """ Scrapes equity data from Wall Street Journal website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        table_header_eq = ['Ticker', 'Total Shareholders Equity', 'Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']
        df_eq = pd.DataFrame(columns=table_header_eq)
        wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
        wsj_end_bs = '/financials/annual/balance-sheet'
        no_data_eq = []
        for i in tickers_df['Symbol']:
            try: 
                wsj_url = wsj_start_bs + i + wsj_end_bs
                response = requests.get(wsj_url, headers=headers)
                soup = BeautifulSoup(response.text, "lxml")
                wants = soup.find('div', class_='collapsed')
                table = wants.find('table', attrs={'class':'cr_dataTable'})
                df_eq = table_content_equity(i, table, df_eq, table_header_eq)
                
            except AttributeError as e:
                no_data_eq.append(i)
        
        df_eq[['Year2021','Year2020', 'Year2019', 'Year2018', 'Year2017']] = df_eq[['Year2021','Year2020', 'Year2019', 'Year2018', 'Year2017']].astype(float)
    
        return df_eq

    def dividends_scraping_data(**kwargs):
        """ Scrapes dividends data from Wall Street Journal website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        table_header_div = ['Ticker', 'Total Cash Dividends', 'Year2021', 'Year2020', 'Year2019', 'Year2018', 'Year2017']
        df_div = pd.DataFrame(columns=table_header_div)
        wsj_start_cf = 'https://www.wsj.com/market-data/quotes/SG/'
        wsj_end_cf = '/financials/annual/cash-flow'
        no_data_div = []
        for i in tickers_df['Symbol']:
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
                
                
            except AttributeError as e:
                no_data_div.append(i)
             
        df_div[['Year2021','Year2020', 'Year2019', 'Year2018', 'Year2017']] = df_div[['Year2021','Year2020', 'Year2019', 'Year2018', 'Year2017']].astype(float)
       
        return df_div

    def inflation_scraping_data(**kwargs):
        """ Scrapes inflation rate data from Rate Inflation website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        url = 'https://www.rateinflation.com/inflation-rate/singapore-inflation-rate/'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "lxml")
        rows = soup.find_all('tr')
        final = []
        for row in rows:
            col = row.find_all('td')
            col = [ele.text.replace('%', '') for ele in col]
            final.append(col[::len(col)-1])
        init = final[2:6]
        df = pd.DataFrame(init, columns=['Year', 'Inflation'])
        return df

    #################################
    # Functions for Scrape          #
    # Yearly                        #
    #################################

    def income_scraping_data_yearly(**kwargs):
        """ Scrapes yearly (up-to-date) net income data from Wall Street Journal website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        # set the column name to 'prev_year_data' so that it can be dynamically called during transformation (on a yearly basis).
        # for example, it is not ideal to name the column as 'year2022' as manual code updates would have to be made yearly to match the year. 
        table_header = ['Ticker', 'Net Income', 'prev_year_data']
        df = pd.DataFrame(columns=table_header)
        wsj_start = 'https://www.wsj.com/market-data/quotes/SG/'
        wsj_end = '/financials/annual/income-statement'
        no_data = []
        for i in tickers_df['Symbol']:
            try: 
                wsj_url = wsj_start + i + wsj_end
                response = requests.get(wsj_url, headers=headers)
                soup = BeautifulSoup(response.text, "lxml")
                wants = soup.find_all("tr")
                table = soup.find('table', attrs={'class':'cr_dataTable'})
                df = table_content_income_yearly(i, table, df, table_header)
                
            except AttributeError as e:
                no_data.append(i)
              
        x = 'prev_year_data'
        df[x] = df[x].astype(float)
        return df

    def assets_scraping_data_yearly(**kwargs):
        """ Scrapes yearly (up-to-date) assets data from Wall Street Journal website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        # set the column name to 'prev_year_data' so that it can be dynamically called during transformation (on a yearly basis).
        table_header_assets = ['Ticker', 'Total Assets', 'prev_year_data']
        df_assets = pd.DataFrame(columns=table_header_assets)
        wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
        wsj_end_bs = '/financials/annual/balance-sheet'
        no_data_assets = []
        for i in tickers_df['Symbol']:
            try: 
                wsj_url = wsj_start_bs + i + wsj_end_bs
                response = requests.get(wsj_url, headers=headers)
                soup = BeautifulSoup(response.text, "lxml")
                wants = soup.find_all("tr")
                table = soup.find('table', attrs={'class':'cr_dataTable'})
                df_assets = table_content_assets_yearly(i, table, df_assets, table_header_assets)
              
            except AttributeError as e:
                no_data_assets.append(i)
               

        x = 'prev_year_data'
        df_assets[x] = df_assets[x].astype(float)
        return df_assets

    def liab_scraping_data_yearly(**kwargs):
        """ Scrapes yearly (up-to-date) liabilities data from Wall Street Journal website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        # set the column name to 'prev_year_data' so that it can be dynamically called during transformation (on a yearly basis).
        table_header_liab = ['Ticker', 'Total Liabilities', 'prev_year_data']
        df_liab = pd.DataFrame(columns=table_header_liab)
        wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
        wsj_end_bs = '/financials/annual/balance-sheet'
        no_data_liab = []
        for i in tickers_df['Symbol']:
            try: 
                wsj_url = wsj_start_bs + i + wsj_end_bs
                response = requests.get(wsj_url, headers=headers)
                soup = BeautifulSoup(response.text, "lxml")
                wants = soup.find('div', class_='collapsed')
                table = wants.find('table', attrs={'class':'cr_dataTable'})
                df_liab = table_content_liab_yearly(i, table, df_liab, table_header_liab)
              
                
            except AttributeError as e:
                no_data_liab.append(i)
                
        x = 'prev_year_data'
        df_liab[x] = df_liab[x].astype(float)
        return df_liab

    def equity_scraping_data_yearly(**kwargs):
        """ Scrapes yearly (up-to-date) equity data from Wall Street Journal website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        # set the column name to 'prev_year_data' so that it can be dynamically called during transformation (on a yearly basis).
        table_header_eq = ['Ticker', 'Total Shareholders Equity', 'prev_year_data']
        df_eq = pd.DataFrame(columns=table_header_eq)
        wsj_start_bs = 'https://www.wsj.com/market-data/quotes/SG/'
        wsj_end_bs = '/financials/annual/balance-sheet'
        no_data_eq = []
        for i in tickers_df['Symbol']:
            try: 
                wsj_url = wsj_start_bs + i + wsj_end_bs
                response = requests.get(wsj_url, headers=headers)
                soup = BeautifulSoup(response.text, "lxml")
                wants = soup.find('div', class_='collapsed')
                table = wants.find('table', attrs={'class':'cr_dataTable'})
                df_eq = table_content_equity_yearly(i, table, df_eq, table_header_eq)
               
                
            except AttributeError as e:
                no_data_eq.append(i)
              
        
        x = 'prev_year_data'
        df_eq[x] = df_eq[x].astype(float)
        return df_eq

    def dividends_scraping_data_yearly(**kwargs):
        """ Scrapes yearly (up-to-date) dividends data from Wall Street Journal website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
        # set the column name to 'prev_year_data' so that it can be dynamically called during transformation (on a yearly basis).
        table_header_div = ['Ticker', 'Total Cash Dividends', 'prev_year_data']
        df_div = pd.DataFrame(columns=table_header_div)
        wsj_start_cf = 'https://www.wsj.com/market-data/quotes/SG/'
        wsj_end_cf = '/financials/annual/cash-flow'
        no_data_div = []
        for i in tickers_df['Symbol']:
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
             
                
            except AttributeError as e:
                no_data_div.append(i)
               
    
        x = 'prev_year_data'
        df_div[x] = df_div[x].astype(float)
        return df_div

    def inflation_scraping_data_yearly(**kwargs):
        """ Scrapes yearly (up-to-date) inflation rate data from Rate Inflation website

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        dataframe 
        
        """
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

    def if_d_financials_exists(**kwargs):
        """ Checks if D_FINANCIALS is empty

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        boolean 
            TRUE - D_FINANCIALS is not empty
            FALSE - D_FINANCIALS is empty 
        
        """
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            query = 'select COUNT(*) from `stockprediction-344203.stock_prediction_datawarehouse.D_FINANCIALS`'
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return True
            else:
                return False
        except:
            return False

    def if_d_inflation_exists(**kwargs):
        """ Checks if D_INFLATION is empty

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        boolean 
            TRUE - D_INFLATION is not empty
            FALSE - D_INFLATION is empty 
        
        """
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            query = 'select COUNT(*) from `stockprediction-344203.stock_prediction_datawarehouse.D_INFLATION`'
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return True
            else:
                return False
        except:
            return False

    ############################
    # Define Python Functions  #
    # For Scraping             #
    ############################

    def scrape_netincome():
        """ Decides whether to scrape historical or yearly (up-to-date) net income.
        If D_FINANCIALS is empty, this function scrapes historical net income.
        If D_FINANCIALS is not empty, this function scrapes yearly (up-to-date) net income. 

        Returns
        -------
        dataframe 
           
        """
        check_dwh = if_d_financials_exists()
        if check_dwh:
            netincome_df = income_scraping_data_yearly()
        else: 
            netincome_df = income_scraping_data()
        return netincome_df
    
    def scrape_assets():
        """ Decides whether to scrape historical or yearly (up-to-date) assets.
        If D_FINANCIALS is empty, this function scrapes historical assets.
        If D_FINANCIALS is not empty, this function scrapes yearly (up-to-date) assets. 

        Returns
        -------
        dataframe 
           
        """
        check_dwh = if_d_financials_exists()
        if check_dwh:
            assets_df = assets_scraping_data_yearly()
        else: 
            assets_df = assets_scraping_data()
        return assets_df

    def scrape_liab():
        """ Decides whether to scrape historical or yearly (up-to-date) liabilities. 
        If D_FINANCIALS is empty, this function scrapes historical liabilities.
        If D_FINANCIALS is not empty, this function scrapes yearly (up-to-date) liabilities. 

        Returns
        -------
        dataframe 
           
        """
        check_dwh = if_d_financials_exists()
        if check_dwh:
            liab_df = liab_scraping_data_yearly()
        else: 
            liab_df = liab_scraping_data()
        return liab_df

    def scrape_equity():
        """ Decides whether to scrape historical or yearly (up-to-date) equity.
        If D_FINANCIALS is empty, this function scrapes historical equity.
        If D_FINANCIALS is not empty, this function scrapes yearly (up-to-date) equity. 

        Returns
        -------
        dataframe 
           
        """
        check_dwh = if_d_financials_exists()
        if check_dwh:
            equity_df = equity_scraping_data_yearly()
        else: 
            equity_df = equity_scraping_data()
        return equity_df

    def scrape_dividends():
        """ Decides whether to scrape historical or yearly (up-to-date) dividends.
        If D_FINANCIALS is empty, this function scrapes historical dividends.
        If D_FINANCIALS is not empty, this function scrapes yearly (up-to-date) dividends. 

        Returns
        -------
        dataframe 
           
        """
        check_dwh = if_d_financials_exists()
        if check_dwh:
            div_df = dividends_scraping_data_yearly()
        else: 
            div_df = dividends_scraping_data()
        return div_df
    
    def scrape_inflation():
        """ Decides whether to scrape historical or yearly (up-to-date) inflation rate.
        If D_INFLATION is empty, this function scrapes historical inflation rate.
        If D_INFLATION is not empty, this function scrapes yearly (up-to-date) inflation rate. 

        Returns
        -------
        dataframe 
           
        """
        check_dwh = if_d_inflation_exists()
        if check_dwh:
            inflation_df = inflation_scraping_data_yearly()
        else: 
            inflation_df = inflation_scraping_data()
        return inflation_df

    #################################
    # Airflow Operators             #
    # For Scraping                  #
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

    [income_scraping, assets_scraping, liab_scraping, equity_scraping, dividends_scraping, inflation_scraping]
    
    return financials_extract_taskgroup