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

    # curr_date = datetime.today().strftime('%Y-%m-%d')

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
        df.reset_index(drop=True, inplace = True) 
        return df

    #################################
    # Functions for Scrape          #
    # (Initialisation - Historical) #
    #################################

    # Function to scrape yahoo finance news (initialise)
    def yahoofinance_scraping_data_init():
        vdisplay = Xvfb()
        vdisplay.start()

        # Chrome driver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--dns-prefetch-disable")
        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

        # scrape news for each ticker
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

                # scrolling
                height = driver.execute_script("return document.documentElement.scrollHeight")
                for i in range(height):
                    driver.execute_script('window.scrollBy(0,20)') # scroll by 20 on each iteration
                    height = driver.execute_script("return document.documentElement.scrollHeight") 
                    # reset height to the new height after scroll-triggered elements have been loaded. 
                test = driver.find_elements(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li[*]/div/div/div[*]/h3/a')
                for i in range(len(test)):
                    link = ''
                    date = ''
                    title = ''
                    index = i+1

                    # get links
                    elems_links = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                    link =elems_links.get_attribute("href")

                    # get titles
                    elems_titles = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                    title =elems_titles.text

                    # get dates
                    try:
                        elems_dates = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/div/span[2]')
                        date = elems_dates.text
                    # ad
                    except NoSuchElementException as e:
                        pass
                    finally: 
                        row = [ticker, title, date, link]
                        df_final.loc[len(df_final)] = row
            except TimeoutException as e:
                pass

        print('scrape successfully')
        driver.quit()

        df_final = clean_df(df_final)

        # remove row if it is ad
        df_final.replace("", np.nan, inplace=True)
        df_final.dropna(subset=['Date'], inplace = True)
        df_final['Source'] = 'Yahoo Finance News'
        df_final['Comments'] = ''
        df_final.reset_index(drop=True, inplace = True) 

        return df_final

    # Function to scrape sg investor news (initialise)
    def sginvestor_scraping_data_init():
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

        # Chrome driver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--dns-prefetch-disable")
        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

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
                cleaned_source.append(src) # will need to double check

        # Convert to DataFrame
        cols = ['Title','Date','Link','Source','Comments']
        df_final = pd.DataFrame({'Title': news_header,
                        'Date': updated_sg_time,
                        'Link': url,
                        'Source': cleaned_source,
                        'Comments': 'Featured on SGInvestors'}, columns=cols)
        df_final.insert(0, 'Ticker', 'None (General News)')
        df_final = clean_df(df_final)

        return df_final

    # Function to scrape sg investor blog news (initialise)
    def sginvestor_blog_scraping_data_init():
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
        # Chrome driver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--dns-prefetch-disable")
        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

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
                for i in range(10):                 # 5 Best Rated Articles and 5 Most Popular Articles
                    description.append('')
                    
                for desc in page_soup.findAll('div',{'class':'description'}): # Latest Articles on Page 1
                    description.append(desc.text)
                    
            else:                                   # Latest Articles from Page 2
                for desc in page_soup.findAll('div',{'class':'description'}):
                    description.append(desc.text)
            
            # updated sg time - only Latest Articles have this
            if num_pages == 1:
                for i in range(10):                 # 5 Best Rated Articles and 5 Most Popular Articles
                    updated_sg_time.append('')
                    
                for time in page_soup.findAll('div',{'class':'updatedsgtime'}): # Latest Articles on Page 1
                    updated_sg_time.append(time.text)
                    
            else:                                   # Latest Articles from Page 2
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
        # drop rows without date
        df_final.replace("", np.nan, inplace=True)
        df_final.dropna(subset=['Date'], inplace = True)        
        df_final = clean_df(df_final)

        return df_final      

    # Function to scrape business times news (initialise)
    def businesstimes_scraping_data_init():

        vdisplay = Xvfb()
        vdisplay.start()

        # Access URL and Handle Advertisement Pop-up
        bt_url = 'https://www.businesstimes.com.sg/keywords/straits-times-index'

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_extension('/home/airflow/airflow/dags/gighmmpiobklfepjocnamgkkbiglidom-4.44.0-Crx4Chrome.com.crx')

        driver = webdriver.Chrome(ChromeDriverManager().install(), options = chrome_options)
        driver.get(bt_url)
        WebDriverWait(driver, 20).until(EC.number_of_windows_to_be(2))

        # 2 Tabs will open - The Business Times and AdBlocker Installation > Switch to Business Times tab
        driver.switch_to.window(driver.window_handles[1])
        driver.save_screenshot('/home/airflow/airflow/dags/screen1.png')
        time.sleep(10)

        # Say 'No thanks' to subscription pop-up if it appears
        try:
            subscription_button = driver.find_element(By.XPATH,'//*[@id="ei_subscribe"]/div/div/div/div[2]/span')
            if subscription_button:
                subscription_button.click()
        except ElementNotInteractableException as e1:
            print('Nothing to press')
        except NoSuchElementException as e2:
            print('Nothing to press')
        
        # Press 'MORE STORIES' until the end to get all possible articles
        more_stories = True
        while more_stories:
            try:
                find_button = driver.find_element(By.XPATH, '//*[@id="block-system-main"]/div/div[2]/div/div[3]/div/div/ul/li/a')
                if find_button != None:
                    find_button.click()
                    time.sleep(3)
                else:
                    more_stories = False
            except StaleElementReferenceException as e1:
                continue
            except ElementClickInterceptedException as e2:
                continue
            except NoSuchElementException as e3:
                print('Page fully loaded')
                break

        # Scrape information
        page_soup = BeautifulSoup(driver.page_source)
        information_block = page_soup.find('div',{'class':'col-lg-8 col-md-8 col-sm-12 col-xs-12 hidden--widget-action classy-panel-styles region'})

        # Initialisations
        news_header = []
        news_description = []
        news_date = []
        url = []

        for header in information_block.findAll('h2',{'class':'widget__title'}):
            header_text = header.find('a').text
            news_header.append(header_text)
            
        for desc in information_block.findAll('div',{'class':'widget__description'}):
            desc_text = desc.find('p').text
            news_description.append(desc_text)
            
        for date in information_block.findAll('div',{'class':'widget__date updated'}):
            news_date.append(date.text)
            
        for header in information_block.findAll('h2',{'class':'widget__title'}):
            href = header.find('a').get('href')
            href = 'https://www.businesstimes.com.sg' + href
            url.append(href)

        print('scrape successfully')
        driver.quit()
        cleaned_time = [date.strip('\n') for date in news_date]
        cleaned_time = [datetime.strptime(date, '%b %d, %Y %I:%M %p') for date in cleaned_time]
        cleaned_time = [date.strftime('%Y-%m-%d') for date in cleaned_time]

        # Convert to DataFrame
        cols = ['Title','Date','Link','Source','Comments']
        df = pd.DataFrame({'Title': news_header,
                        'Date': cleaned_time,
                        'Link': url,
                        'Source': 'The Business Times',
                        'Comments': ''}, columns=cols)
        df.insert(0, 'Ticker', 'None (General News)')

        df['Title'] = df['Title'].astype(str).str.replace("'", "")
        df['Title'] = df['Title'].astype(str).str.replace('"', '')

        return df

    #################################
    # Functions for Scrape          #
    # (Daily)                       #
    #################################

    def yahoofinance_scraping_data_daily(pulled_date):
        vdisplay = Xvfb()
        vdisplay.start()

        # Chrome driver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--dns-prefetch-disable")
        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

        # scrape news for each ticker
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

                # scrolling
                height = driver.execute_script("return document.documentElement.scrollHeight")
                for i in range(height):
                    driver.execute_script('window.scrollBy(0,20)') # scroll by 20 on each iteration
                    height = driver.execute_script("return document.documentElement.scrollHeight") 
                    # reset height to the new height after scroll-triggered elements have been loaded. 
                test = driver.find_elements(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li[*]/div/div/div[*]/h3/a')
                
                for i in range(len(test)):
                    link = ''
                    date = ''
                    title = ''
                    index = i+1
                    
                    # get dates
                    try:
                        elems_dates = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/div/span[2]')
                        date = elems_dates.text
                    except NoSuchElementException as e:
                        pass
                    finally: 
                        date = clean_date(date)
                    try: 
                        date = date.strftime('%Y-%m-%d') 
                    # is an ad
                    except AttributeError as e:
                        pass
                    # scrape only news that are within limit
                    finally:
                        if date =='':
                            pass
                        elif date < pulled_date.strftime('%Y-%m-%d'):
                            break

                        # get links
                        elems_links = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                        link =elems_links.get_attribute("href")

                        # get titles
                        elems_titles = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                        title =elems_titles.text

                        row = [ticker, title, date, link]
                        df_final.loc[len(df_final)] = row

            except TimeoutException as e:
                pass

        print('scrape successfully')
        driver.quit()
        
        # remove row if it is ad
        df_final.replace("", np.nan, inplace=True)
        df_final.dropna(subset=['Date'], inplace = True)
        df_final['Source'] = 'Yahoo Finance News'
        df_final['Comments'] = ''
        df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
        df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')
        df_final.reset_index(drop=True, inplace = True) 
        return df_final


    def sginvestor_scraping_data_daily(pulled_date):
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
        # Chrome driver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--dns-prefetch-disable")
        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

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
                cleaned_source.append(src) # will need to double check

        # Convert to DataFrame
        cols = ['Title','Date','Link','Source','Comments']
        df_final = pd.DataFrame({'Title': news_header,
                        'Date': updated_sg_time,
                        'Link': url,
                        'Source': cleaned_source,
                        'Comments': 'Featured on SGInvestors'}, columns=cols)
        df_final.insert(0, 'Ticker', 'None (General News)')

        # Cleaning
        df_final['Date'] = df_final['Date'].apply(clean_date)
        df_final['Date'] = df_final['Date'].dt.strftime('%Y-%m-%d')
        # only scrape latest news
        df_final = check_date(df_final, pulled_date)
        df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
        df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')

        return df_final

    def sginvestor_blog_scraping_data_daily(pulled_date):
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
        # Chrome driver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--dns-prefetch-disable")
        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

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
                for i in range(10):                 # 5 Best Rated Articles and 5 Most Popular Articles
                    description.append('')
                    
                for desc in page_soup.findAll('div',{'class':'description'}): # Latest Articles on Page 1
                    description.append(desc.text)
                    
            else:                                   # Latest Articles from Page 2
                for desc in page_soup.findAll('div',{'class':'description'}):
                    description.append(desc.text)
            
            # updated sg time - only Latest Articles have this
            if num_pages == 1:
                for i in range(10):                 # 5 Best Rated Articles and 5 Most Popular Articles
                    updated_sg_time.append('')
                    
                for time in page_soup.findAll('div',{'class':'updatedsgtime'}): # Latest Articles on Page 1
                    updated_sg_time.append(time.text)
                    
            else:                                   # Latest Articles from Page 2
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
        # drop rows without date
        df_final.replace("", np.nan, inplace=True)
        df_final.dropna(subset=['Date'], inplace = True)

        # Cleaning
        df_final['Date'] = df_final['Date'].apply(clean_date)
        df_final['Date'] = df_final['Date'].dt.strftime('%Y-%m-%d')
        df_final = check_date(df_final, pulled_date)
        df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
        df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')

        return df_final

    # Function to scrape business times news 
    def businesstimes_scraping_data_daily(pulled_date):

        vdisplay = Xvfb()
        vdisplay.start()

        # Access URL and Handle Advertisement Pop-up
        bt_url = 'https://www.businesstimes.com.sg/keywords/straits-times-index'

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_extension('/home/airflow/airflow/dags/gighmmpiobklfepjocnamgkkbiglidom-4.44.0-Crx4Chrome.com.crx')

        driver = webdriver.Chrome(ChromeDriverManager().install(), options = chrome_options)
        driver.get(bt_url)
        WebDriverWait(driver, 20).until(EC.number_of_windows_to_be(2))

        # 2 Tabs will open - The Business Times and AdBlocker Installation > Switch to Business Times tab
        driver.switch_to.window(driver.window_handles[1])
        driver.save_screenshot('/home/airflow/airflow/dags/screen1.png')
        time.sleep(10)

        # Say 'No thanks' to subscription pop-up if it appears
        try:
            subscription_button = driver.find_element(By.XPATH,'//*[@id="ei_subscribe"]/div/div/div/div[2]/span')
            if subscription_button:
                subscription_button.click()
        except ElementNotInteractableException as e1:
            print('Nothing to press')
        except NoSuchElementException as e2:
            print('Nothing to press')
        
        # Press 'MORE STORIES' until the end to get all possible articles
        more_stories = True
        while more_stories:
            try:
                find_button = driver.find_element(By.XPATH, '//*[@id="block-system-main"]/div/div[2]/div/div[3]/div/div/ul/li/a')
                if find_button != None:
                    find_button.click()
                    time.sleep(3)
                else:
                    more_stories = False
            except StaleElementReferenceException as e1:
                continue
            except ElementClickInterceptedException as e2:
                continue
            except NoSuchElementException as e3:
                print('Page fully loaded')
                break

        # Scrape information
        page_soup = BeautifulSoup(driver.page_source)
        information_block = page_soup.find('div',{'class':'col-lg-8 col-md-8 col-sm-12 col-xs-12 hidden--widget-action classy-panel-styles region'})

        # Initialisations
        news_header = []
        news_description = []
        news_date = []
        url = []

        for header in information_block.findAll('h2',{'class':'widget__title'}):
            header_text = header.find('a').text
            news_header.append(header_text)
            
        for desc in information_block.findAll('div',{'class':'widget__description'}):
            desc_text = desc.find('p').text
            news_description.append(desc_text)
            
        for date in information_block.findAll('div',{'class':'widget__date updated'}):
            news_date.append(date.text)
            
        for header in information_block.findAll('h2',{'class':'widget__title'}):
            href = header.find('a').get('href')
            href = 'https://www.businesstimes.com.sg' + href
            url.append(href)

        print('scrape successfully')
        driver.quit()
        cleaned_time = [date.strip('\n') for date in news_date]
        cleaned_time = [datetime.strptime(date, '%b %d, %Y %I:%M %p') for date in cleaned_time]
        cleaned_time = [date.strftime('%Y-%m-%d') for date in cleaned_time]

        # Convert to DataFrame
        cols = ['Title','Date','Link','Source','Comments']
        df = pd.DataFrame({'Title': news_header,
                        'Date': cleaned_time,
                        'Link': url,
                        'Source': 'The Business Times',
                        'Comments': ''}, columns=cols)
        df.insert(0, 'Ticker', 'None (General News)')

        df['Title'] = df['Title'].astype(str).str.replace("'", "")
        df['Title'] = df['Title'].astype(str).str.replace('"', '')
        df = check_date(df, pulled_date)
        df.reset_index(drop=True, inplace = True)  

        return df

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
        string_date = recent_date.strftime('%Y-%m-%d')
        print('string_date', string_date)
        return string_date

    def scrape_yahoofinance():
        check_dwh = if_f_news_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            yahoofinance_df = yahoofinance_scraping_data_daily(pulled_date)
            print("Scrape yahoofinance daily")
        else: 
            yahoofinance_df = yahoofinance_scraping_data_init()
            print("Scrape yahoofinance init")
        return yahoofinance_df

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

    def scrape_businesstimes():
        check_dwh = if_f_news_exists()
        if check_dwh:
            pulled_date = get_recent_date()
            businesstimes_df = businesstimes_scraping_data_daily(pulled_date)
            print("Scrape business times daily")
        else: 
            businesstimes_df = businesstimes_scraping_data_init()
            print("Scrape business times init")
        return businesstimes_df

    ########################
    # Airflow Operators    #
    ########################

    # Scraping yahoo finance news 
    yahoofinance_scraping = PythonOperator(
        task_id = 'yahoofinance_scraping',
        python_callable = scrape_yahoofinance,
        dag = dag
    )

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

    # Scraping business times news 
    businesstimes_scraping = PythonOperator(
        task_id = 'businesstimes_scraping',
        python_callable = scrape_businesstimes,
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
        trigger_rule="all_done",
        dag=dag
    )

    # TASK DEPENDENCIES

    start_pipeline >> [yahoofinance_scraping, sginvestor_scraping, sginvestor_blog_scraping, businesstimes_scraping] >> prep_gcs
# [yahoofinance_scraping, sginvestor_scraping, sginvestor_blog_scraping, businesstimes_scraping]
    return extract_taskgroup