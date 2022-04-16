from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from google.cloud import storage
from google.cloud import bigquery
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from xvfbwrapper import Xvfb

import json
import logging
import numpy as np
import os
import pandas as pd
import requests
import time

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'

logging.basicConfig(level=logging.INFO)

def build_extract_news_taskgroup(dag: DAG) -> TaskGroup:
    """Creates a taskgroup for extraction of data from various sources

    Parameters
    ----------
    dag: An airflow DAG

    Returns
    -------
    taskgroup
        A taskgroup that contains all the functions and operators
    """
    extract_news_taskgroup = TaskGroup(group_id = 'extract_news_taskgroup')

    # Load tickers that will be used
    tickers_df = pd.read_csv('/home/airflow/airflow/dags/sti.csv')

    ############################
    # Define Python Functions  #
    ############################

    #################################
    # Helper Functions for Scrape   #
    # (Initialisation)              #
    #################################

    def clean_df(df_final):
        """Helper function to clean the dates and titles

        Parameters
        ----------
        df_final: dataframe
            Dataframe of extracted news

        Returns
        -------
        dataframe
            Cleaned dataframe of extracted news
        """
        cleaned_time = []
        current_time = datetime.today()
        for t in df_final['Date']:
            t = t.strip('(edited) ')
            if t == '':
                cleaned_time.append('')
            else:
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
                    last_update = t 
                last_update = last_update.strftime('%Y-%m-%d') 
                cleaned_time.append(last_update)
        df_final['Date'] = cleaned_time   
        df_final.reset_index(drop=True, inplace = True)  
        df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
        df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')
        return df_final

    #################################
    # Helper Functions for Scrape   #
    # (Daily)                       #
    #################################

    def clean_date(date):
        """Helper function to clean the dates

        Parameters
        ----------
        date: str
            Relative date of each extracted news

        Returns
        -------
        date
            Exact date of each extracted news based on current date
        """
        date = date.strip('(edited) ')
        current_time = datetime.today()
        if 'minute' in date:
            date = current_time - timedelta(minutes=int(date.split(' ')[0]))
        elif 'hour' in date:
            date = current_time - timedelta(hours=int(date.split(' ')[0]))
        elif 'day' in date:
            if 'yesterday' in date:
                date = current_time - timedelta(days=int(1))
            else:
                date = current_time - timedelta(days=int(date.split(' ')[0]))
        elif 'month' in date:
            if 'last' in date:
                date = current_time + relativedelta(months=-1)
            else:
                date = int(date.split(' ')[0])
                date = current_time + relativedelta(months=-date)
        elif 'year' in date:
            if 'last' in date:
                date = current_time + relativedelta(years=-1)
            else:
                date = int(date.split(' ')[0])
                date = current_time + relativedelta(years=-date)
        return date

    def check_date(df, pulled_date):
        """Helper function to filter the dataframe to get updated news

        Parameters
        ----------
        df: dataframe
            Dataframe of extracted news
        pulled_date: str
            The most recent date present in DWH

        Returns
        -------
        dataframe
            Contains extracted news that are greater than the pulled date
        """
        df = df[(df['Date'] > pulled_date)]
        df.reset_index(drop=True, inplace = True) 
        return df

    ###########################
    # Scraping Initialisation #
    ###########################

    def yahoofinance_scraping_data_init():
        """Function to obtain news articles from Yahoo Finance via Selenium and Beauitful Soup

        Returns
        -------
        dataframe
            Contains details for each news article from initial date till latest date
        """
        # Scraping initialisation
        vdisplay = Xvfb()
        vdisplay.start()
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

        # Scrape news for each ticker
        start = 'https://sg.finance.yahoo.com/quote/'
        end = '/news'
        df_final = pd.DataFrame(columns =['Ticker', 'Title', 'Date', 'Link'])
        
        for ticker in tickers_df['New Symbol']:
            try: 
                link = start + ticker + end
                driver.get(link)
                time.sleep(5)
                print('-----------------')
                print(ticker)

                SCROLL_PAUSE_TIME = 5
                # Get scroll height
                last_height = driver.execute_script("return document.documentElement.scrollHeight")
                while True:
                    # Scroll down to bottom
                    driver.execute_script("window.scrollTo(0,document.documentElement.scrollHeight);")
                    # Wait to load page
                    time.sleep(SCROLL_PAUSE_TIME)
                    # Calculate new scroll height and compare with last scroll height
                    new_height = driver.execute_script("return document.documentElement.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height

                test = driver.find_elements(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li[*]/div/div/div[*]/h3/a')
                for i in range(len(test)):
                    link = ''
                    date = ''
                    title = ''
                    index = i+1
                    # Get links
                    elems_links = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                    link =elems_links.get_attribute("href")
                    # Get titles
                    elems_titles = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                    title =elems_titles.text
                    # Get dates
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

        logging.info('Yahoo Finance scrape successfully')
        driver.quit()
        df_final = clean_df(df_final)

        # Remove row if it is an ad
        df_final.replace("", np.nan, inplace=True)
        df_final.dropna(subset=['Date'], inplace = True)
        df_final['Source'] = 'Yahoo Finance News'
        df_final['Comments'] = ''
        df_final.reset_index(drop=True, inplace = True) 
        return df_final

    def yahoofinance_scraping_data_daily(pulled_date):
        """Function to obtain news articles from Yahoo Finance via Selenium and Beauitful Soup

        Parameters
        ----------
        pulled_date: str
            The most recent date present in DWH

        Returns
        -------
        dataframe
            Contain details for each news article since the last date in dwh
        """
        # Scraping initialisation
        vdisplay = Xvfb()
        vdisplay.start()
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

        # Scrape news for each ticker
        start = 'https://sg.finance.yahoo.com/quote/'
        end = '/news'
        df_final = pd.DataFrame(columns =['Ticker', 'Title', 'Date', 'Link'])
        current_time = datetime.today()
        limit = current_time + relativedelta(days=-1)

        for ticker in tickers_df['New Symbol']:
            try: 
                link = start + ticker + end
                driver.get(link)
                time.sleep(5)
                print('-----------------')
                print(ticker)

                SCROLL_PAUSE_TIME = 5
                # Get scroll height
                last_height = driver.execute_script("return document.documentElement.scrollHeight")
                while True:
                    # Scroll down to bottom
                    driver.execute_script("window.scrollTo(0,document.documentElement.scrollHeight);")
                    # Wait to load page
                    time.sleep(SCROLL_PAUSE_TIME)
                    # Calculate new scroll height and compare with last scroll height
                    new_height = driver.execute_script("return document.documentElement.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                test = driver.find_elements(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li[*]/div/div/div[*]/h3/a')
                
                for i in range(len(test)):
                    link = ''
                    date = ''
                    title = ''
                    index = i+1
                    # Get dates
                    try:
                        elems_dates = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/div/span[2]')
                        date = elems_dates.text
                    except NoSuchElementException as e:
                        pass
                    finally: 
                        date = clean_date(date)

                    try: 
                        date = datetime.datetime.strptime(date, '%Y-%m-%d')
                    except AttributeError as e:
                        pass
                    # Scrape news that > latest date from dwh
                    finally:
                        if date == '':
                            pass
                        elif date < datetime.strptime(pulled_date, '%Y-%m-%d'):
                            break

                        # Get links
                        elems_links = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                        link =elems_links.get_attribute("href")
                        # Get titles
                        elems_titles = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                        title = elems_titles.text
                        row = [ticker, title, date, link]
                        df_final.loc[len(df_final)] = row
            except TimeoutException as e:
                pass

        logging.info('Yahoo Finance scrape successfully')
        driver.quit()
        
        # Remove row if it is ad
        df_final.replace("", np.nan, inplace=True)
        df_final.dropna(subset=['Date'], inplace = True)
        df_final['Source'] = 'Yahoo Finance News'
        df_final['Comments'] = ''
        df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
        df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')
        df_final.reset_index(drop=True, inplace = True) 
        return df_final

    def helper_sginvestor():
        """Helper function to obtain news articles from SG Investor via Selenium and Beauitful Soup

        Returns
        -------
        dataframe
            Contain details for each news article 
        """
        # Obtain page URLs (only 8 pages)
        page_url_list = ['https://sginvestors.io/news/publishers/latest/'] # 1st page
        for i in range(2,9): # for 8 pages
            page_url_list.append('https://sginvestors.io/news/publishers/latest/0' + str(i))

        # Initialisations
        news_source = []
        news_header = []
        updated_sg_time = []
        url = []
        num_pages = 1

        # Scraping initialisation
        vdisplay = Xvfb()
        vdisplay.start()
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

        # Scraping of pages
        for page_url in page_url_list[:]:
            driver.get(page_url)
            page_soup = BeautifulSoup(driver.page_source,"html.parser")
            print('Page Number: ', num_pages)
            print('Page URL: ', page_url)
            # Get news_source
            for source in page_soup.findAll('img',{'class':'newschannelimg'}):
                source_link = source['src']
                news_source.append(source_link)
            # Get news header / title
            for header in page_soup.findAll('div',{'class':'newstitle'}):
                news_header.append(header.text)
            # Get updated sg time
            for time in page_soup.findAll('div',{'class':'updatedsgtime'}):
                updated_sg_time.append(time.text)
            # Get url
            link_container = page_soup.find('div',{'id':'articlelist'})
            for news_url in link_container.findAll('a',{'rel':'nofollow'}):
                href = news_url.get('href')
                url.append(href)
            num_pages += 1
        logging.info('Sg Investor scrape successfully')
        driver.quit()

        # Clean up Source information
        cleaned_source = []
        for src in news_source:
            if 'cna' in src:
                cleaned_source.append('CNA')
            elif 'theedgegroup' in src:
                cleaned_source.append('The Edge')
            elif 'business-times' in src:
                cleaned_source.append('The Business Times')
            else:
                cleaned_source.append(src) 

        # Convert to DataFrame
        cols = ['Title','Date','Link','Source','Comments']
        df_final = pd.DataFrame({'Title': news_header,
                        'Date': updated_sg_time,
                        'Link': url,
                        'Source': cleaned_source,
                        'Comments': 'Featured on SGInvestors'}, columns=cols)
        df_final.insert(0, 'Ticker', 'None (General News)')
        return df_final

    def sginvestor_scraping_data_init():
        """Function to obtain news articles from SG Investor via Selenium and Beauitful Soup

        Returns
        -------
        dataframe
            Contain details for each news article from initial date till latest date
        """
        df_final = helper_sginvestor()
        df_final = clean_df(df_final)
        return df_final

    def sginvestor_scraping_data_daily(pulled_date):
        """Function to obtain news articles from SG Investor via Selenium and Beauitful Soup

        Parameters
        ----------
        pulled_date: str
            The most recent date present in DWH

        Returns
        -------
        dataframe
            Contain details for each news article since the last date in dwh
        """
        df_final = helper_sginvestor()
        # Cleaning
        df_final['Date'] = df_final['Date'].apply(clean_date)
        df_final['Date'] = df_final['Date'].dt.strftime('%Y-%m-%d')
        # Only scrape latest news
        df_final = check_date(df_final, pulled_date)
        df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
        df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')
        return df_final

    def helper_sginvestor_blog():
        """Helper function to obtain news articles from SG Investor Blog via Selenium and Beauitful Soup

        Returns
        -------
        dataframe
            Contain details for each news article 
        """
        # Obtain page URLs
        page_url_list = ['https://research.sginvestors.io/p/bloggers-say.html'] # 1st page
        for i in range(2,7): # for 6 pages
            page_url_list.append('https://research.sginvestors.io/p/bloggers-say-' + str(i) + '.html')

        # Initialisations
        source = []
        author = []
        title = []
        description = []
        updated_sg_time = []
        url = []
        num_pages = 1

        # Scraping initialisation
        vdisplay = Xvfb()
        vdisplay.start()
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

        # Scraping of pages
        for page_url in page_url_list[:]:
            driver.get(page_url)
            page_soup = BeautifulSoup(driver.page_source,"html.parser")
            print('Page Number: ', num_pages)
            print('Page URL: ', page_url)
            # Get source
            for src in page_soup.findAll('div',{'class':'blogtitle'}):
                src_span = src.find('span')
                source.append(src_span.text)
            # Get author
            for auth in page_soup.findAll('div',{'class':'authorname'}):
                author.append(auth.text)
            # Get title
            for ttl in page_soup.findAll('div',{'class':'title'}):
                title.append(ttl.text)
            # Get description - only Latest Articles have this
            if num_pages == 1:
                for i in range(10):                 
                    description.append('')
                for desc in page_soup.findAll('div',{'class':'description'}): 
                    description.append(desc.text)
            else:                                   
                for desc in page_soup.findAll('div',{'class':'description'}):
                    description.append(desc.text)
            
            if num_pages == 1:
                for i in range(10):                 
                    updated_sg_time.append('')
                for time in page_soup.findAll('div',{'class':'updatedsgtime'}): 
                    updated_sg_time.append(time.text) 
            else:                                   
                for time in page_soup.findAll('div',{'class':'updatedsgtime'}):
                    updated_sg_time.append(time.text)
            
            # Get url
            for url_item in page_soup.findAll('article',{'class':'bloggeritem'}):
                all_link_lst = url_item.get('onclick')
                link_lst = all_link_lst[3:-2].split(',')
                http_lst = [string for string in link_lst if 'http' in string]
                http = [string.replace('"', '') for string in http_lst]
                http_clean = [string.replace("'", "") for string in http]
                url.append(http_clean[-1].strip(" "))
            num_pages += 1

        logging.info('Sg Investor Blog scrape successfully')
        driver.quit()

        # Convert to DataFrame
        cols = ['Title','Date','Link','Source','Comments']
        df_final = pd.DataFrame({'Title': title,
                        'Date': updated_sg_time,
                        'Link': url,
                        'Source': source,
                        'Comments': 'Blogs posts featured on SGInvestors'}, columns=cols)
        df_final.insert(0, 'Ticker', 'None (General News)')
        print('len', len(df_final))

        # Drop rows without date
        df_final.replace("", np.nan, inplace=True)
        df_final.dropna(subset=['Date'], inplace = True)   
        return df_final

    def sginvestor_blog_scraping_data_init():
        """Function to obtain news articles from SG Investor Blog via Selenium and Beauitful Soup

        Returns
        -------
        dataframe
            Contain details for each news article from initial date till latest date
        """
        df_final = helper_sginvestor_blog()
        df_final = clean_df(df_final)
        return df_final     

    def sginvestor_blog_scraping_data_daily(pulled_date):
        """Function to obtain news articles from SG Investor Blog via Selenium and Beauitful Soup

        Parameters
        ----------
        pulled_date: str
            The most recent date present in DWH

        Returns
        -------
        dataframe
            Contain details for each news article since the last date in dwh
        """
        df_final = helper_sginvestor_blog()
        # Cleaning
        df_final['Date'] = df_final['Date'].apply(clean_date)
        df_final['Date'] = df_final['Date'].dt.strftime('%Y-%m-%d')
        df_final = check_date(df_final, pulled_date)
        df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
        df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')
        return df_final

    #############
    # Check DWH #
    #############

    def if_f_news_exists():
        """Check if News Fact Table is present in DWHS

            Returns
            -------
            bool
        """
        try:
            # Establish connection with BigQuery 
            bq_client = bigquery.Client()
            query = 'select COUNT(`Date`) from `stockprediction-344203.stock_prediction_datawarehouse.F_NEWS`'

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
        """Get the most recent date present in News Fact Table

        Returns
        -------
        str
            The most recent date present in DWH, inform users to extract data after that date
        """
        # Establish connection with bigquery
        bq_client = bigquery.Client()
        query = "select MAX(`Date`) from `stockprediction-344203.stock_prediction_datawarehouse.F_NEWS`"
        df = bq_client.query(query).to_dataframe()
        recent_date = df['f0_'].values[0]
        string_date = np.datetime_as_string(recent_date, unit='D')
        return string_date

    def scrape_yahoofinance():
        """Get Yahoo Finance news based on condiitons

        Returns
        -------
        dataframe
            Contain Yahoo Finance news 
        """
        check_dwh = if_f_news_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            yahoofinance_df = yahoofinance_scraping_data_daily(pulled_date)
            logging.info("Scrape Yahoo Finance daily")
        else: 
            yahoofinance_df = yahoofinance_scraping_data_init()
            logging.info("Scrape Yahoo Finance init")
        return yahoofinance_df

    def scrape_sginvestor():
        """Get SG Investor news based on condiitons

        Returns
        -------
        dataframe
            Contain SG Investor news 
        """
        check_dwh = if_f_news_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            sginvestor_df = sginvestor_scraping_data_daily(pulled_date)
            logging.info("Scrape SG Investor daily")
        else: 
            sginvestor_df = sginvestor_scraping_data_init()
            logging.info("Scrape SG Investor init")
        return sginvestor_df

    def scrape_sginvestor_blog():
        """Get SG Investor Blog news based on condiitons

        Returns
        -------
        dataframe
            Contain SG Investor Blog news 
        """
        check_dwh = if_f_news_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            sginvestor_blog_df = sginvestor_blog_scraping_data_daily(pulled_date)
            plogging.info("Scrape SG Investor Blog daily")
        else: 
            sginvestor_blog_df = sginvestor_blog_scraping_data_init()
            logging.info("Scrape SG Investor Blog init")
        return sginvestor_blog_df

    ####################
    # Define Operators #
    ####################

    # Scraping Yahoo Finance
    yahoofinance_scraping = PythonOperator(
        task_id = 'yahoofinance_scraping',
        python_callable = scrape_yahoofinance,
        dag = dag
    )

    # Scraping SG Investor
    sginvestor_scraping = PythonOperator(
        task_id = 'sginvestor_scraping',
        python_callable = scrape_sginvestor,
        dag = dag
    )

    # Scraping SG Investor Blog
    sginvestor_blog_scraping = PythonOperator(
        task_id = 'sginvestor_blog_scraping',
        python_callable = scrape_sginvestor_blog,
        dag = dag
    )

    [yahoofinance_scraping, sginvestor_scraping, sginvestor_blog_scraping]
    return extract_news_taskgroup

