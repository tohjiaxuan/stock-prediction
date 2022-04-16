from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
import logging
from pandas_datareader import data as pdr

import json
import os
import numpy as np
import pandas as pd
import pandas_ta as ta
import requests
import urllib.request
import yfinance as yf 

logging.basicConfig(level=logging.INFO)

yf.pdr_override()

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'

def build_extract_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup for extraction of data from various sources

    Parameters
    ----------
    dag: An airflow DAG

    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    extract_taskgroup = TaskGroup(group_id = 'extract_taskgroup')
    
    # Obtain current day
    curr_date = datetime.today().strftime('%Y-%m-%d')

    # Load tickers that will be used
    tickers_df = pd.read_csv('/home/airflow/airflow/dags/sti.csv')

    ############################
    # Define Python Functions  #
    ############################
    def helper_retrieval(link, headers):
        """Helper function to obtain records from MAS

        Parameters
        ----------
        link: str
            The URL of the website 
        
        headers: dict
            Dictionary of required elements for BS to be used

        Returns
        -------
        list
            Information required in list of dictionaries
        """
        req = urllib.request.Request(link, None, headers)
        response = urllib.request.urlopen(req)
        data = response.read()
        raw_data = json.loads(data.decode()) # Need to decode to get values
        batch = raw_data['result']['records']
        return batch
    
    def helper_latest_retrieval(website_link):
        """Helper function to obtain latest date of record in MAS

        Parameters
        ----------
        website_link: str
            The URL of the website 

        Returns
        -------
        datetime
            Date of most recent record stored on MAS website
        """
        latest_retrival = helper_retrieval(website_link, headers)
        latest_mas_date = latest_retrival[0]['end_of_day']
        latest_mas_date = datetime.strptime(latest_mas_date, '%Y-%m-%d')
        return latest_mas_date

    #############
    # Check DWH #
    #############
    
    def if_f_stock_exists():
        """Check if Stocks Fact Table is present in DWHS

        Returns
        -------
        bool
        """
        try:
            # Establish connection with BigQuery 
            bq_client = bigquery.Client()
            query = 'select COUNT(`Date`) from `stockprediction-344203.stock_prediction_datawarehouse.F_STOCKS`'

            # Convert queried result to df
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return True
            else:
                return False
        except:
            return False

    def get_recent_date():
        """Get the most recent date present in Stocks Fact Table

        Returns
        -------
        str
            The most recent date present in DWH, inform users to extract data after that date
        """
        # Establish connection with bigquery
        bq_client = bigquery.Client()
        query = "select MAX(`Date`) from `stockprediction-344203.stock_prediction_datawarehouse.F_STOCKS`"
        df = bq_client.query(query).to_dataframe()
        recent_date = df['f0_'].values[0]
        string_date = np.datetime_as_string(recent_date, unit='D')
        return string_date

    def if_d_commodities_exists():
        """Check if Commodities Dimension Table is present in DWHS

        Returns
        -------
        bool
        """
        try:
            bq_client = bigquery.Client()
            query = 'select COUNT(`Date`) from `stockprediction-344203.stock_prediction_datawarehouse.D_COMMODITIES`'
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return True
            else:
                return False
        except:
            return False

    def get_recent_commodities_date():
        """Get the most recent date present in Commodities Dimension Table

        Returns
        -------
        strs
            The most recent date present in DWH, inform users to extract data after that date
        """
        bq_client = bigquery.Client()
        query = "select MAX(`Date`) from `stockprediction-344203.stock_prediction_datawarehouse.D_COMMODITIES`"
        df = bq_client.query(query).to_dataframe()
        recent_date = df['f0_'].values[0]
        string_date = np.datetime_as_string(recent_date, unit='D')
        return string_date

    ###########################
    # Scraping Initialisation #
    ###########################
    # Function to obtain yfinance historial prices

    def helper_stock_price(tickers, start_date, end_date):
        """Get the most recent date present in Commodities Dimension Table
        
        Parameters
        ----------
        tickers: dataframe
            All the ticker values for each stock
        start_date:str
            Oldest date required to extract stocks from
        end_date: str
            The date of DAG being triggered

        Returns
        -------
        str
            The most recent date present in DWH, inform users to extract data after that date
        """
        sgx = list(tickers['New Symbol']) 
        stocks =[]
        print("Retrieve stocks from:", start_date, "till ", end_date)
        # Loop to get all historical prices
        for ticker in sgx:
            curr_ticker = yf.Ticker(ticker)
            curr_df = curr_ticker.history(start = start_date, end = end_date)

            # Check if df contains results
            if len(curr_df) == 0:
                logging.info(ticker, " no information available in timeframe")
                continue

            # Check for duplicated indices (timestamp) and remove them
            curr_df = curr_df.loc[~curr_df.index.duplicated(keep='last')]
            curr_df = curr_df.reset_index()
            curr_df['Stock'] = ticker
            stocks.append(curr_df)
        
        if len(stocks) == 0:
            # Create empty dataframe
            col_names = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends',
            'Stock_Splits', 'Stock']
            df = pd.DataFrame(columns=col_names)
        else:
            logging.info('Combining all stocks data into a dataframe')   
            # Concatenate all dfs
            df = pd.concat(stocks)
        return df

    def initialise_stock_price(start_date, end_date):
        """Get stocks data from 2018 to initialisation date
        
        Parameters
        ----------
        start_date: str
            Fixed to be 2 Jan 2018 (1 Jan is a holiday, no data)
        end_date: str
            The date of DAG being triggered

        Returns
        -------
        dataframe
            Contain stock prices and information from start date to end date
        """
        stock_df = helper_stock_price(tickers_df, start_date, end_date)
        logging.info("Extracted stock prices to initialise DWH")
        return stock_df

    def update_stock_price(start_date, end_date):
        """Get most recent stocks data
        
        Parameters
        ----------
        start_date: str
            One day after latest date recorded in DWH
        end_date: str
            The date of DAG being triggered

        Returns
        -------
        dataframe
            Contain stock prices and information from start date to end date
        """
        stock_df = helper_stock_price(tickers_df, start_date, end_date)
        logging.info("Extracted daily stock prices (Update DWH)")
        return stock_df
    
    def stock_prices():
        """Get stocks data based on condiitons

        Returns
        -------
        dataframe
            Contain stock prices and information from start date to end date
        """
        # Check if fact table for stocks was created
        check_dwh = if_f_stock_exists()
        if check_dwh:
            logging.info("Fact table exists, proceed to get start_date")
            pulled_date = get_recent_date()
            start_date = (datetime.strptime(pulled_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            logging.info("Start to extract stock prices (Update DWH")
            stock_df = update_stock_price(start_date, curr_date)
        else:
            logging.info("Start to extract stocks data to populate DWH")
            stock_df = initialise_stock_price('2018-01-02', curr_date)
        return stock_df

    def helper_exchange_rate(initial_date):
        """Helper function to obtain exchange rate data from MAS (API)
        
        Parameters
        ----------
        initial_date: str
            Signify the oldest date that data is required from

        Returns
        -------
        dataframe
            Contain exchange rate details from initial date till latest date
        """
        oldest_datetime_obj = datetime.strptime(initial_date, '%Y-%m-%d')
        
        # Obtain latest 1000 rows, limit set by MAS
        curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=95932927-c8bc-4e7a-b484-68a66a24edfe&limit=1000&sort=end_of_day%20desc'
        batch1 = helper_retrieval(curr_link, headers)
        
        er_init_batch = batch1.copy() # To prevent modifications made to er_init_batch
        
        counter = 0
        # Check if new date is inside the 1000 records
        for record in batch1:
            curr_date = datetime.strptime(record['end_of_day'], '%Y-%m-%d')
            if record['end_of_day'] == initial_date:
                return pd.DataFrame(batch1[0:counter+1])
            elif curr_date < oldest_datetime_obj:
                return pd.DataFrame(batch1[0:counter])
            else:
                counter += 1

        # If date of interest is more than 1000 days before triggered date
        curr_old = batch1[999]['end_of_day'] 
        
        while(curr_old != initial_date):
            # Update new end_date and start date
            new_end = datetime.strptime(curr_old, '%Y-%m-%d') - timedelta(days=1)
            new_start = new_end - timedelta(days=1000)
            
            # If less than 1000 records or just nice 1000 days away, then can just use oldest
            if new_start <= oldest_datetime_obj:
                logging.info("Less than 1000 days from initialisation date")
                date_url = '&between[end_of_day]=2018-01-01,'+ new_end.strftime('%Y-%m-%d')
                curr_old = '2018-01-02'
                
            else:
                logging.info("More than 1000 days from initialisation date")
                date_url = '&between[end_of_day]=' + new_start.strftime('%Y-%m-%d') + ','+ new_end.strftime('%Y-%m-%d')
            
            # Get new requests based on update URL
            new_url = curr_link + date_url
            curr_batch = helper_retrieval(new_url, headers)
            index = len(curr_batch)

            # If unable to retrieve anything, stop loop
            if index == 0:
                break
            er_init_batch = er_init_batch + curr_batch
            
            # Update condition:
            if curr_old == initial_date:
                break
            else:
                curr_old = curr_batch[index-1]['end_of_day']
        
        df = pd.DataFrame(er_init_batch)
        return df

    def initialise_exchange_rate(start_date):
        """Get exchange rate data from 2018 to initialisation date
        
        Parameters
        ----------
        start_date: str
            Fixed at 2 Jan 2018 (1 Jan is a holiday, no data)

        Returns
        -------
        dataframe
            Contain exchange rate prices and information from 2018-01-02 to triggered date
        """
        ex_df = helper_exchange_rate(start_date)
        logging.info("Extracted exchange rates to initialise DWH")
        return ex_df

    def update_exchange_rate(pulled_date):
        """Get most recent exchange rate data
        
        Parameters
        ----------
        pulled_date: str
            Latest date recorded in DWH

        Returns
        -------
        dataframe
            Contain exchange rate data from start date to end date
        """
        start_date = (datetime.strptime(pulled_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        # Check if the most recent date in the data source is >= to the new start_date
        curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=95932927-c8bc-4e7a-b484-68a66a24edfe&limit=1&sort=end_of_day%20desc'
        latest_mas_date = helper_latest_retrieval(curr_link)
        if latest_mas_date >= datetime.strptime(start_date, '%Y-%m-%d'):
            ex_df = helper_exchange_rate(start_date)
            logging.info("Extracted daily exchange rate (Update DWH)")

        else:
            # Create empty dataframe if dataframe cannot be found
            col_names = ['end_of_day', 'eur_sgd', 'gbp_sgd', 'usd_sgd', 'aud_sgd', 'cad_sgd', 'cny_sgd_100',
            'hkd_sgd_100', 'inr_sgd_100', 'idr_sgd_100', 'jpy_sgd_100', 'krw_sgd_100', 'myr_sgd_100',
            'twd_sgd_100', 'nzd_sgd', 'php_sgd_100', 'qar_sgd_100', 'sar_sgd_100', 'chf_sgd', 'thb_sgd_100',
            'aed_sgd_100', 'vnd_sgd_100', 'preliminary', 'timestamp']
            ex_df = pd.DataFrame(columns=col_names)
            logging.info("No new exchange rate data to collect")
        return ex_df

    def exchange_rate():
        """Get exchange rate data based on condiitons

        Returns
        -------
        dataframe
            Contain exchange rate data from start date to end date
        """
        # Check if fact table exists as dimension tables are created along with it
        check_dwh = if_f_stock_exists() 
        if check_dwh:
            pulled_date = get_recent_date()
            logging.info('Start to extract exchange rate data (Update)')
            ex_df = update_exchange_rate(pulled_date)
        else:
            logging.info('Start to extract initial exchange rate data')
            ex_df = initialise_exchange_rate('2018-01-02')
        return ex_df

    def helper_interest_rate(initial_date):
        """Helper function to obtain interest rate data from MAS (API)
        
        Parameters
        ----------
        initial_date: str
            Signify the oldest date that data is required from

        Returns
        -------
        dataframe
            Contain interest rate details from initial date till latest date
        """
        oldest_datetime_obj = datetime.strptime(initial_date, '%Y-%m-%d')
        
        # Obtain latest 1000 rows
        curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=1000&sort=end_of_day%20desc'
        batch1 = helper_retrieval(curr_link, headers)
        
        ir_init_batch = batch1.copy()

        counter = 0
        # Check if date of interest is within 1000 records
        for record in batch1:
            curr_date = datetime.strptime(record['end_of_day'], '%Y-%m-%d')
            if record['end_of_day'] == initial_date:
                return pd.DataFrame(batch1[0:counter+1])
            elif curr_date < oldest_datetime_obj:
                return pd.DataFrame(batch1[0:counter])
            else:
                counter += 1

        curr_old = batch1[999]['end_of_day']
        
        while(curr_old != initial_date):
            # Update new end_date and start date
            new_end = datetime.strptime(curr_old, '%Y-%m-%d') - timedelta(days=1)
            new_start = new_end - timedelta(days=1000)
            
            # If less than 1000 records or just nice 1000 days away, then can just use oldest
            if new_start <= oldest_datetime_obj:
                logging.info("Less than 1000 days from initialisation date")
                date_url = '&between[end_of_day]=2018-01-01,'+ new_end.strftime('%Y-%m-%d')
                curr_old = '2018-01-02'
                
            else:
                logging.info("More than 1000 days from initialisation date")
                date_url = '&between[end_of_day]=' + new_start.strftime('%Y-%m-%d') + ','+ new_end.strftime('%Y-%m-%d')
            
            # Get new requests
            new_url = curr_link + date_url
            curr_batch = helper_retrieval(new_url, headers)
            index = len(curr_batch)
            if index == 0:
                break
            ir_init_batch = ir_init_batch + curr_batch
            
            # Update condition:
            if curr_old == initial_date:
                break
            else:
                curr_old = curr_batch[index-1]['end_of_day']

        df = pd.DataFrame(ir_init_batch)
        df = df[df['sora'].notna()] # If sora is NA, data not required, usually means not updated on MAS
        return df
    
    def initialise_interest_rate(start_date):
        """Get interest rate data from 2018 to initialisation date
        
        Parameters
        ----------
        start_date: str
            Fixed at 2 Jan 2018 (1 Jan is a holiday, no data)

        Returns
        -------
        dataframe
            Contain interest rate prices and information from 2018-01-02 to triggered date
        """
        int_df = helper_interest_rate(start_date)
        logging.info("Extracted interest rates to initialise DWH")
        return int_df
    
    def update_interest_rate(pulled_date):
        """Get most recent interest rate data
        
        Parameters
        ----------
        pulled_date: str
            Latest date recorded in DWH

        Returns
        -------
        dataframe
            Contain interest rate data from start date to end date
        """
        start_date = (datetime.strptime(pulled_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=1&sort=end_of_day%20desc'
        latest_mas_date = helper_latest_retrieval(curr_link)
        if latest_mas_date >= datetime.strptime(start_date, "%Y-%m-%d"):
            int_df = helper_interest_rate(start_date)
            logging.info("Extracted daily interest rate (Update DWH)")
        else:
            # Create empty dataframe
            col_names = ['end_of_day', 'interbank_overnight', 'interbank_1w', 'interbank_1m', 'interbank_2m', '_interbank_3m',
            'interbank_6m', 'interbank_12m', 'commercial_bills_3m', 'usd_sibor_3m', 'sgs_repo_overnight_rate', 'standing_facility_deposit',
            'rmb_overnight_rate', 'sora', 'sora_index', 'comp_sora_1m', 'comp_sora_3m', 'comp_sora_6m',
            'aggregate_volume', 'highest_transaction', 'lowest_transaction', 'calculation_method', 
            'on_rmb_facility_rate','preliminary', 'published_date','sor_average','standing_facility_borrow','timestamp']
            logging.info("No new interest rate data to collect")
            int_df = pd.DataFrame(columns=col_names)
        return int_df

    def pull_older_interest_rate(pulled_date):
        """Get interest rate data that is one day before pulled_date
        
        Parameters
        ----------
        pulled_date: str
            Latest date recorded in DWH

        Returns
        -------
        dataframe
            Contain interest rate data from one day before pulled_date
        """
        curr_link = 'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&limit=1000&sort=end_of_day%20desc'
        # Retrieve interest rate data from 7 days before till latest date in DWH
        new_start = datetime.strptime(pulled_date, '%Y-%m-%d') - timedelta(days=7)
        new_end = datetime.strptime(pulled_date, '%Y-%m-%d')
        date_url = '&between[end_of_day]=' + new_start.strftime('%Y-%m-%d') + ','+ new_end.strftime('%Y-%m-%d')
        
        # Retrieve data from MAS API
        new_url = curr_link + date_url
        new_batch = helper_retrieval(new_url, headers)
        df = pd.DataFrame(new_batch)

        # Check if df.head(1) is same as pulled date
        temp = df.head(1)
        # If it is same as pulled date, not what we want, get the data one row after it
        # Data is sorted in descending order
        if (temp.iloc[0]['end_of_day'] == pulled_date):
            df = df.iloc[[1]]
        else:
            df = temp
        return df
        
    def interest_rate():
        """Get interest rate data based on condiitons

        Returns
        -------
        dataframe
            Contain interest rate data from start date to end date
        """
        # Check if fact table exists as dimension tables are created along with it
        check_dwh = if_f_stock_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            logging.info('Start to extract interest rate data (Update)')
            t_lag_df = pull_older_interest_rate(pulled_date)
            logging.info('Pulled lagged interest rate (Update)')
            t_df = update_interest_rate(pulled_date)
        else:
            logging.info('Start extraction of interest rate to populate DWH')
            t_lag_df = pull_older_interest_rate('2018-01-02')
            logging.info('Pulled lagged interest rate')
            t_df = initialise_interest_rate('2018-01-02')
        int_df = t_df.append(t_lag_df, ignore_index = True)
        
        if len(int_df) == 1 & int_df['sora'].isna().values.any():
            int_df = int_df.iloc[0:0]
        
        int_df = int_df.drop_duplicates('end_of_day')
        return int_df

    def initialise_gold_prices(start_date, end_date):
        """Get gold prices data from 2018 to initialisation date
        
        Parameters
        ----------
        start_date: str
            Fixed at 2 Jan 2018 (1 Jan is a holiday, no data)
        end_date: str
            Date of initialisation of DWH / Triggered DAG

        Returns
        -------
        dataframe
            Contain gold prices and information from 2018-01-02 to triggered date
        """
        gold_df = pdr.get_data_yahoo("GC=F", start=start_date, end=end_date)
        gold_df = gold_df.reset_index() # so 'Date' becomes a column
        logging.info("Extracted gold prices to initialise DWH")
        return gold_df

    def update_gold_prices(pulled_date, curr_date):
        """Get most recent gold prices data
        
        Parameters
        ----------
        pulled_date: str
            Latest date recorded in DWH
        curr_date: str
            Date where DAG was triggered

        Returns
        -------
        dataframe
            Contain gold prices data from pulled date to curr date
        """
        gold_df = pdr.get_data_yahoo("GC=F", start=pulled_date, end=curr_date)
        gold_df = gold_df.reset_index() # so 'Date' becomes a column
        logging.info("Extracted daily gold prices (Update DWH)")
        return gold_df

    def gold_prices():
        """Get gold data based on condiitons

        Returns
        -------
        dataframe
            Contain gold data from start date to end date
        """
        check_dwh = if_d_commodities_exists() # Check if dimension table exists
        if check_dwh:
            pulled_date = get_recent_commodities_date()
            logging.info('Start to extract gold prices data (Update)')
            gold_df = update_gold_prices(pulled_date, curr_date)
        else:
            logging.info('Start extraction of gold prices data to populate DWH')
            gold_df = initialise_gold_prices('2018-01-02', curr_date)
        return gold_df

    def initialise_silver_prices(start_date, end_date):
        """Get silver prices data from 2018 to initialisation date
        
        Parameters
        ----------
        start_date: str
            Fixed at 2 Jan 2018 (1 Jan is a holiday, no data)
        end_date: str
            Date of initialisation of DWH / Triggered DAG

        Returns
        -------
        dataframe
            Contain silver prices and information from 2018-01-02 to triggered date
        """
        silver_df = pdr.get_data_yahoo("SI=F", start=start_date, end=end_date)
        silver_df = silver_df.reset_index() # so 'Date' becomes a column
        logging.info("Extracted silver prices to initialise DWH")
        return silver_df

    def update_silver_prices(pulled_date, curr_date):
        """Get most recent silver prices data
        
        Parameters
        ----------
        pulled_date: str
            Latest date recorded in DWH
        curr_date: str
            Date where DAG was triggered

        Returns
        -------
        dataframe
            Contain silver prices data from pulled date to curr date
        """
        silver_df = pdr.get_data_yahoo("SI=F", start=pulled_date, end=curr_date)
        silver_df = silver_df.reset_index() # so 'Date' becomes a column
        logging.info("Extracted daily silver prices (Update DWH)")
        return silver_df

    def silver_prices():
        """Get silver data based on condiitons

        Returns
        -------
        dataframe
            Contain silver data from start date to end date
        """
        check_dwh = if_d_commodities_exists() # Check if dimension table exists
        if check_dwh:
            pulled_date = get_recent_commodities_date()
            logging.info('Start to extract silver prices data (Update)')
            silver_df = update_silver_prices(pulled_date, curr_date)
        else:
            logging.info('Start extraction of silver prices data to populate DWH')
            silver_df = initialise_silver_prices('2018-01-02', curr_date)
        return silver_df

    def initialise_crude_oil_prices(start_date, end_date):
        """Get crude oil prices data from 2018 to initialisation date
        
        Parameters
        ----------
        start_date: str
            Fixed at 2 Jan 2018 (1 Jan is a holiday, no data)
        end_date: str
            Date of initialisation of DWH / Triggered DAG

        Returns
        -------
        dataframe
            Contain crude oil prices and information from 2018-01-02 to triggered date
        """
        crude_oil_df = pdr.get_data_yahoo("CL=F", start=start_date, end=end_date)
        crude_oil_df = crude_oil_df.reset_index() # so 'Date' becomes a column
        logging.info("Extracted crude oil prices to initialise DWH")
        return crude_oil_df

    def update_crude_oil_prices(pulled_date, curr_date):
        """Get most recent crude oil prices data
        
        Parameters
        ----------
        pulled_date: str
            Latest date recorded in DWH
        curr_date: str
            Date where DAG was triggered

        Returns
        -------
        dataframe
            Contain crude oil prices data from pulled date to curr date
        """
        crude_oil_df = pdr.get_data_yahoo("CL=F", start=pulled_date, end=curr_date)
        crude_oil_df = crude_oil_df.reset_index() # so 'Date' becomes a column  
        logging.info("Extracted daily crude oil prices (Update DWH)")
        return crude_oil_df

    def crude_oil_prices():
        """Get crude oil data based on condiitons

        Returns
        -------
        dataframe
            Contain crude oil data from start date to end date
        """
        check_dwh = if_d_commodities_exists() # Check if dimension table exists
        if check_dwh:
            pulled_date = get_recent_commodities_date()
            logging.info('Start to extract crude oil prices data (Update)')
            crude_oil_df = update_crude_oil_prices(pulled_date, curr_date)
        else:
            logging.info('Start extraction of crude oil prices data to populate DWH')
            crude_oil_df = initialise_crude_oil_prices('2018-01-02', curr_date)
        return crude_oil_df

    ####################
    # Define Operators #
    ####################

    #################
    # Extract Stage #
    #################

    # Scraping Historical Stock Prices
    stock_scraping = PythonOperator(
        task_id = 'stock_scraping_data',
        python_callable = stock_prices,
        dag = dag
    )

    # Scraping Daily Exchange Rates
    exchange_rate_scraping = PythonOperator(
        task_id = 'exchange_rate_scraping_data',
        python_callable = exchange_rate,
        dag = dag
    )

    # Scraping Daily Interest Rates
    interest_rate_scraping = PythonOperator(
        task_id = 'interest_rate_scraping_data',
        python_callable = interest_rate,
        dag = dag
    )

    # Scraping Daily Gold Prices
    gold_scraping = PythonOperator(
        task_id = 'gold_scraping_data',
        python_callable = gold_prices,
        dag = dag
    )

    # Scraping Daily Silver Prices
    silver_scraping = PythonOperator(
        task_id = 'silver_scraping_data',
        python_callable = silver_prices,
        dag = dag
    )

    # Scraping Daily Crude Oil Prices
    crude_oil_scraping = PythonOperator(
        task_id = 'crude_oil_scraping_data',
        python_callable = crude_oil_prices,
        dag = dag
    )


    [stock_scraping, exchange_rate_scraping, interest_rate_scraping, gold_scraping, silver_scraping, crude_oil_scraping]

    return extract_taskgroup