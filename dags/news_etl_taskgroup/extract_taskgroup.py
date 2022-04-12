from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
from airflow.utils.task_group import TaskGroup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from xvfbwrapper import Xvfb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from webdriver_manager.firefox import GeckoDriverManager
from selenium import webdriver
from pyvirtualdisplay import Display

import json
import os
import numpy as np
import pandas as pd
import requests
import time

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bq_client = bigquery.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'

def build_extract_taskgroup(dag: DAG) -> TaskGroup:
    extract_taskgroup = TaskGroup(group_id = 'extract_taskgroup')

    # Load tickers that will be used
    tickers_df = pd.read_csv('/home/airflow/airflow/dags/sti.csv')

    #################################
    # Helper Functions for Scrape   #
    # (Initialisation)              #
    #################################

    # Function to clean the dates and titles
    def clean_df(df_final):
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
                    last_update = t # will need to double check
                    print('check', t)
                    
                last_update = last_update.strftime('%Y-%m-%d') 
                cleaned_time.append(last_update)
        df_final['Date'] = cleaned_time   
        df_final.reset_index(drop=True, inplace = True)  
        df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
        df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')
        print('clean successfully')  
        return df_final

    #################################
    # Helper Functions for Scrape   #
    # (Daily)                       #
    #################################

    def clean_date(date):
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

    # Function to scrape news that > latest date from dwh
    def check_date(df, pulled_date):
        df = df[(df['Date'] > pulled_date)]
        print('check date', len(df))
        df.reset_index(drop=True, inplace = True) 
        return df

    #################################
    # Functions for Scrape          #
    #################################

    # Function to scrape yahoo finance news (initialise)
    # def yahoofinance_scraping_data_init():
    #     vdisplay = Xvfb()
    #     vdisplay.start()

    #     options = webdriver.FirefoxOptions()
    #     options.add_argument('--headless')
    #     driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

    #     # # Chrome driver
    #     # chrome_options = webdriver.ChromeOptions()
    #     # chrome_options.add_argument("--dns-prefetch-disable")
    #     # driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

    #     # scrape news for each ticker
    #     start = 'https://sg.finance.yahoo.com/quote/'
    #     end = '/news'
    #     df_final = pd.DataFrame(columns =['Ticker', 'Title', 'Date', 'Link'])
        
    #     for ticker in tickers_df['New Symbol']:
    #         try: 
    #             link = start + ticker + end
    #             driver.get(link)
    #             time.sleep(5)
    #             print('-----------------')
    #             print(ticker)

    #             # scrolling
    #             height = driver.execute_script("return document.documentElement.scrollHeight")
    #             for i in range(height):
    #                 driver.execute_script('window.scrollBy(0,30)') # scroll by 20 on each iteration
    #                 height = driver.execute_script("return document.documentElement.scrollHeight") 
    #                 # reset height to the new height after scroll-triggered elements have been loaded. 
    #             test = driver.find_elements(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li[*]/div/div/div[*]/h3/a')
    #             for i in range(len(test)):
    #                 link = ''
    #                 date = ''
    #                 title = ''
    #                 index = i+1

    #                 # get links
    #                 elems_links = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
    #                 link =elems_links.get_attribute("href")

    #                 # get titles
    #                 elems_titles = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
    #                 title =elems_titles.text

    #                 # get dates
    #                 try:
    #                     elems_dates = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/div/span[2]')
    #                     date = elems_dates.text
    #                 # ad
    #                 except NoSuchElementException as e:
    #                     pass
    #                 finally: 
    #                     row = [ticker, title, date, link]
    #                     df_final.loc[len(df_final)] = row
    #         except TimeoutException as e:
    #             pass

    #     print('scrape successfully')
    #     driver.quit()

    #     df_final = clean_df(df_final)

    #     # remove row if it is ad
    #     df_final.replace("", np.nan, inplace=True)
    #     df_final.dropna(subset=['Date'], inplace = True)
    #     df_final['Source'] = 'Yahoo Finance News'
    #     df_final['Comments'] = ''
    #     df_final.reset_index(drop=True, inplace = True) 

    #     return df_final

    # def yahoofinance_scraping_data_daily(pulled_date):
    #     vdisplay = Xvfb()
    #     vdisplay.start()

    #     options = webdriver.FirefoxOptions()
    #     options.add_argument('--headless')
    #     driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

    #     # # Chrome driver
    #     # chrome_options = webdriver.ChromeOptions()
    #     # chrome_options.add_argument("--dns-prefetch-disable")
    #     # driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

    #     # scrape news for each ticker
    #     start = 'https://sg.finance.yahoo.com/quote/'
    #     end = '/news'
    #     df_final = pd.DataFrame(columns =['Ticker', 'Title', 'Date', 'Link'])
    #     current_time = datetime.today()
    #     limit = current_time + relativedelta(days=-1)

    #     for ticker in tickers_df['New Symbol']:
    #         try: 
    #             link = start + ticker + end
    #             driver.get(link)
    #             time.sleep(5)
    #             print('-----------------')
    #             print(ticker)

    #             # scrolling
    #             height = driver.execute_script("return document.documentElement.scrollHeight")
    #             for i in range(height):
    #                 driver.execute_script('window.scrollBy(0,30)') # scroll by 20 on each iteration
    #                 height = driver.execute_script("return document.documentElement.scrollHeight") 
    #                 # reset height to the new height after scroll-triggered elements have been loaded. 
    #             test = driver.find_elements(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li[*]/div/div/div[*]/h3/a')
                
    #             for i in range(len(test)):
    #                 link = ''
    #                 date = ''
    #                 title = ''
    #                 index = i+1
                    
    #                 # get dates
    #                 try:
    #                     elems_dates = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/div/span[2]')
    #                     date = elems_dates.text
    #                 except NoSuchElementException as e:
    #                     pass
    #                 finally: 
    #                     date = clean_date(date)
    #                 try: 
    #                     date = datetime.datetime.strptime(date, '%Y-%m-%d')
    #                 # is an ad
    #                 except AttributeError as e:
    #                     pass
    #                 # scrape news that > latest date from dwh
    #                 finally:
    #                     if date == '':
    #                         pass
    #                     elif date < datetime.strptime(pulled_date, '%Y-%m-%d'):
    #                         break

    #                     # get links
    #                     elems_links = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
    #                     link =elems_links.get_attribute("href")

    #                     # get titles
    #                     elems_titles = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
    #                     title =elems_titles.text

    #                     row = [ticker, title, date, link]
    #                     df_final.loc[len(df_final)] = row

    #         except TimeoutException as e:
    #             pass

    #     print('scrape successfully')
    #     driver.quit()
        
    #     # remove row if it is ad
    #     df_final.replace("", np.nan, inplace=True)
    #     df_final.dropna(subset=['Date'], inplace = True)
    #     df_final['Source'] = 'Yahoo Finance News'
    #     df_final['Comments'] = ''
    #     df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
    #     df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')
    #     df_final.reset_index(drop=True, inplace = True) 
    #     return df_final

    # Function to scrape sginvestor
    def helper_sginvestor():
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

        vdisplay = Xvfb()
        vdisplay.start()

        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)
        
        # Chrome driver
        # chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument("--no-sandbox") 
        # chrome_options.add_argument("--dns-prefetch-disable")
        # chrome_options.add_argument("--disable-dev-shm-usage")
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--remote-debugging-port=9222")
        # driver = webdriver.Chrome(ChromeDriverManager(version="96.0.4664.18").install(), chrome_options=chrome_options)

        # Scraping of pages
        for page_url in page_url_list[:]:
            driver.get(page_url)
            page_soup = BeautifulSoup(driver.page_source,"html.parser")
            print('Page Number: ', num_pages)
            print('Page URL: ', page_url)
            
            # news_source
            for source in page_soup.findAll('img',{'class':'newschannelimg'}):
                source_link = source['src']
                news_source.append(source_link)
            
            # news header / title
            for header in page_soup.findAll('div',{'class':'newstitle'}):
                news_header.append(header.text)
                
            # updated sg time
            for time in page_soup.findAll('div',{'class':'updatedsgtime'}):
                updated_sg_time.append(time.text)
            
            # url
            link_container = page_soup.find('div',{'id':'articlelist'})
            for news_url in link_container.findAll('a',{'rel':'nofollow'}):
                href = news_url.get('href')
                url.append(href)
            
            num_pages += 1
            print('---------')
        print('---Scraping done!!---')
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
        print('len', len(df_final))
        return df_final

    def sginvestor_scraping_data_init():
        df_final = helper_sginvestor()
        df_final = clean_df(df_final)
        return df_final

    def sginvestor_scraping_data_daily(pulled_date):
        df_final = helper_sginvestor()
        # Cleaning
        df_final['Date'] = df_final['Date'].apply(clean_date)
        df_final['Date'] = df_final['Date'].dt.strftime('%Y-%m-%d')
        # only scrape latest news
        df_final = check_date(df_final, pulled_date)
        df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
        df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')
        return df_final

    # Function to scrape sg investor blog 
    def helper_sginvestor_blog():
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

        vdisplay = Xvfb()
        vdisplay.start()

        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)

        # Chrome driver
        # chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument("--dns-prefetch-disable")
        # driver = webdriver.Chrome(chrome_options=chrome_options)
        # driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

        # Scraping of pages
        for page_url in page_url_list[:]:
            driver.get(page_url)
            page_soup = BeautifulSoup(driver.page_source,"html.parser")
            print('Page Number: ', num_pages)
            print('Page URL: ', page_url)
            
            # source
            for src in page_soup.findAll('div',{'class':'blogtitle'}):
                src_span = src.find('span')
                source.append(src_span.text)
                
            # author
            for auth in page_soup.findAll('div',{'class':'authorname'}):
                author.append(auth.text)
            
            # title
            for ttl in page_soup.findAll('div',{'class':'title'}):
                title.append(ttl.text)
                
            # description - only Latest Articles have this
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
            
            # url
            for url_item in page_soup.findAll('article',{'class':'bloggeritem'}):
                all_link_lst = url_item.get('onclick')
                link_lst = all_link_lst[3:-2].split(',')
                http_lst = [string for string in link_lst if 'http' in string]
                http = [string.replace('"', '') for string in http_lst]
                http_clean = [string.replace("'", "") for string in http]
                url.append(http_clean[-1].strip(" "))
            
            num_pages += 1
            print('---------')
        print('---Scraping done!!---')
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

        # drop rows without date
        df_final.replace("", np.nan, inplace=True)
        df_final.dropna(subset=['Date'], inplace = True)   
        return df_final

    def sginvestor_blog_scraping_data_init():
        df_final = helper_sginvestor_blog()
        df_final = clean_df(df_final)
        return df_final     

    def sginvestor_blog_scraping_data_daily(pulled_date):
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
        try:
            bq_client = bigquery.Client()
            query = 'select COUNT(`Date`) from `stockprediction-344203.stock_prediction_datawarehouse.F_NEWS`'
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return True
            else:
                return False
        except:
            return False

    def get_recent_date():
        bq_client = bigquery.Client()
        query = "select MAX(`Date`) from `stockprediction-344203.stock_prediction_datawarehouse.F_NEWS`"
        df = bq_client.query(query).to_dataframe()
        recent_date = df['f0_'].values[0]
        # string_date = '2022-03-25'
        string_date = datetime.strptime(recent_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        # recent_date.strftime('%Y-%m-%d')
        print('string_date', string_date)
        return string_date

    # def scrape_yahoofinance():
    #     check_dwh = if_f_news_exists()
    #     if check_dwh:
    #         pulled_date = get_recent_date()
    #         yahoofinance_df = yahoofinance_scraping_data_daily(pulled_date)
    #         print("Scrape yahoofinance daily")
    #     else: 
    #         yahoofinance_df = yahoofinance_scraping_data_init()
    #         print("Scrape yahoofinance init")
    #     return yahoofinance_df

    def scrape_sginvestor():
        check_dwh = if_f_news_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            sginvestor_df = sginvestor_scraping_data_daily(pulled_date)
            print("Scrape sginvestor daily")
        else: 
            sginvestor_df = sginvestor_scraping_data_init()
            print("Scrape sginvestor init")
        return sginvestor_df

    
    def scrape_sginvestor_blog():
        check_dwh = if_f_news_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            sginvestor_blog_df = sginvestor_blog_scraping_data_daily(pulled_date)
            print("Scrape sginvestor blog daily")
        else: 
            sginvestor_blog_df = sginvestor_blog_scraping_data_init()
            print("Scrape sginvestor blog init")
        return sginvestor_blog_df

    ########################
    # Airflow Operators    #
    ########################

    # # Scraping yahoo finance news 
    # yahoofinance_scraping = PythonOperator(
    #     task_id = 'yahoofinance_scraping',
    #     python_callable = scrape_yahoofinance,
    #     dag = dag
    # )

    # Scraping sg investor news 
    sginvestor_scraping = PythonOperator(
        task_id = 'sginvestor_scraping',
        python_callable = scrape_sginvestor,
        dag = dag
    )

    # Scraping sg investor blog news 
    sginvestor_blog_scraping = PythonOperator(
        task_id = 'sginvestor_blog_scraping',
        python_callable = scrape_sginvestor_blog,
        dag = dag
    )

    # Start of DAG (to test)
    start_pipeline = BashOperator(
        task_id = 'start_pipeline',
        bash_command = 'echo start',
        dag = dag
    )

    prep_gcs = BashOperator(
        task_id="prep_gcs",
        bash_command="echo prep_gcs",
        # trigger_rule="all_done",
        dag=dag
    )

    # TASK DEPENDENCIES
    start_pipeline >> [sginvestor_scraping, sginvestor_blog_scraping] >> prep_gcs
    # start_pipeline >> [yahoofinance_scraping, sginvestor_scraping, sginvestor_blog_scraping] >> prep_gcs
    return extract_taskgroup

