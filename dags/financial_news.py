from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import pandas as pd
import numpy as np
import time
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, InvalidArgumentException, WebDriverException, NoSuchElementException, ElementClickInterceptedException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from xvfbwrapper import Xvfb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/airflow/airflow/dags/stockprediction_servicekey.json'
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_prediction_is3107')
STAGING_DATASET = 'stock_prediction_staging_dataset'
PROJECT_ID = 'stockprediction-344203'
DWH_DATASET = 'stock_prediction_datawarehouse'

default_args = {
     'start_date': datetime(2022, 3, 12)
    }


dag = DAG(
    dag_id = 'financial_news_scrape',
    description = 'Scraping and collecting financial news of companies',
    default_args = default_args,
    schedule_interval = "@daily",
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
                
            last_update = last_update.strftime('%Y/%m/%d') 
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

# Function to scrape news that > limit (ytd) -> today 
# should call latest date from dwh
def check_date(df):
    current_time = datetime.today()
    limit = current_time + relativedelta(days=-1)
    limit = limit.strftime('%Y/%m/%d')
    df = df[(df['Date'] > limit)]
    df.reset_index(drop=True, inplace = True) 
    return df

#################################
# Functions for Scrape          #
# (Initialisation - Historical) #
#################################

# Function to scrape yahoo finance news (initialise)
def yahoofinance_scraping_data_init(**kwargs):
    vdisplay = Xvfb()
    vdisplay.start()

    # Chrome driver
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(ChromeDriverManager().install(), options = options)

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
def sginvestor_scraping_data_init(**kwargs):
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
    driver = webdriver.Chrome(ChromeDriverManager().install())

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
def sginvestor_blog_scraping_data_init(**kwargs):
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
    driver = webdriver.Chrome(ChromeDriverManager().install())

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
def business_times_scraping_data_init(**kwargs):

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
    cleaned_time = [date.strftime('%Y/%m/%d') for date in cleaned_time]

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

def yahoofinance_scraping_data_daily(**kwargs):
    vdisplay = Xvfb()
    vdisplay.start()

    # Chrome driver
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(ChromeDriverManager().install(), options = options)

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
                    date = date.strftime('%Y/%m/%d') 
                # is an ad
                except AttributeError as e:
                    pass
                # scrape only news that are within limit
                finally:
                    if date =='':
                        pass
                    elif date < limit.strftime('%Y/%m/%d'):
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


def sginvestor_scraping_data_daily(**kwargs):
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
    driver = webdriver.Chrome(ChromeDriverManager().install())

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
    df_final['Date'] = df_final['Date'].dt.strftime('%Y/%m/%d')
    # only scrape latest news
    df_final = check_date(df_final)
    df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
    df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')

    return df_final

def sginvestor_blog_scraping_data_daily(**kwargs):
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
    driver = webdriver.Chrome(ChromeDriverManager().install())

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
    df_final['Date'] = df_final['Date'].dt.strftime('%Y/%m/%d')
    df_final = check_date(df_final)
    df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
    df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')

    return df_final

# Function to scrape business times news 
def business_times_scraping_data_daily(**kwargs):

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
    cleaned_time = [date.strftime('%Y/%m/%d') for date in cleaned_time]

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
    df = check_date(df)
    df.reset_index(drop=True, inplace = True)  

    return df



############################
# Push to GCS From XCOMS #
############################

################################
# Functions for Initialisation #
################################

# Push yahoo finance news from XCOM to Cloud
def push_yahoofinance_news_init(ti):
    curr_data = ti.xcom_pull(task_ids='yahoofinance_scraping_data_init')
    curr_data.to_parquet('gs://stock_prediction_is3107/yahoofinance_news_init.parquet')
    print("Pushing Yahoo Finance News to Cloud (init)")

# Push sg investor news from XCOM to Cloud
def push_sginvestor_news_init(ti):
    curr_data = ti.xcom_pull(task_ids='sginvestor_scraping_data_init')
    curr_data.to_parquet('gs://stock_prediction_is3107/sginvestor_news_init.parquet')
    print("Pushing SG Investor News to Cloud (init)")

# Push sg investor blog news from XCOM to Cloud
def push_sginvestor_blog_news_init(ti):
    curr_data = ti.xcom_pull(task_ids='sginvestor_blog_scraping_data_init')
    curr_data.to_parquet('gs://stock_prediction_is3107/sginvestor_blog_news_init.parquet')
    print("Pushing SG Investor Blog News to Cloud (init)")

# Push business times news from XCOM to Cloud
def push_business_times_news_init(ti):
    curr_data = ti.xcom_pull(task_ids='business_times_scraping_data_init')
    curr_data.to_parquet('gs://stock_prediction_is3107/business_times_news_init.parquet')
    print("Pushing Business Times News to Cloud (init)")

################################
# Functions for Daily          #
################################

# Push yahoo finance news from XCOM to Cloud
def push_yahoofinance_news_daily(ti):
    curr_data = ti.xcom_pull(task_ids='yahoofinance_scraping_data_daily')
    curr_data.to_parquet('gs://stock_prediction_is3107/yahoofinance_news_daily.parquet')
    print("Pushing Yahoo Finance News to Cloud")

# Push sg investor news from XCOM to Cloud
def push_sginvestor_news_daily(ti):
    curr_data = ti.xcom_pull(task_ids='sginvestor_scraping_data_daily')
    curr_data.to_parquet('gs://stock_prediction_is3107/sginvestor_news_daily.parquet')
    print("Pushing SG Investor News to Cloud")

# Push sg investor blog news from XCOM to Cloud
def push_sginvestor_blog_news_daily(ti):
    curr_data = ti.xcom_pull(task_ids='sginvestor_blog_scraping_data_daily')
    curr_data.to_parquet('gs://stock_prediction_is3107/sginvestor_blog_news_daily.parquet')
    print("Pushing SG Investor Blog News to Cloud")

# Push business times news from XCOM to Cloud
def push_business_times_news_daily(ti):
    curr_data = ti.xcom_pull(task_ids='business_times_scraping_data_daily')
    curr_data.to_parquet('gs://stock_prediction_is3107/business_times_news_daily.parquet')
    print("Pushing Business Times News to Cloud")


##################
# Choosing Paths #
##################

# Check if yahoofinance exists in GCS, choose path (init or daily)
def check_yahoofinance_choose(**kwargs):
    yahoofinance = 'yahoofinance_news_init.parquet'
    if (storage.Blob(bucket = bucket, name = yahoofinance).exists(storage_client)):
        return 'scrape_yahoofinance_daily'
    else:
        return 'scrape_yahoofinance_init'

# Check if sginvestor exists in GCS, choose path (init or daily)
def check_sginvestor_choose(**kwargs):
    sginvestor = 'sginvestor_news_init.parquet'
    if (storage.Blob(bucket = bucket, name = sginvestor).exists(storage_client)):
        return 'scrape_sginvestor_daily'
    else:
        return 'scrape_sginvestor_init'

# Check if sginvestor blog exists in GCS, choose path (init or daily)
def check_sginvestor_blog_choose(**kwargs):
    sginvestor_blog = 'sginvestor_blog_news_init.parquet'
    if (storage.Blob(bucket = bucket, name = sginvestor_blog).exists(storage_client)):
        return 'scrape_sginvestor_blog_daily'
    else:
        return 'scrape_sginvestor_blog_init'

# Check if businesstimes exists in GCS, choose path (init or daily)
def check_businesstimes_choose(**kwargs):
    businesstimes = 'business_times_news_init.parquet'
    if (storage.Blob(bucket = bucket, name = businesstimes).exists(storage_client)):
        return 'scrape_businesstimes_daily'
    else:
        return 'scrape_businesstimes_init'

check_yahoofinance_choose_path = BranchPythonOperator(
    task_id = 'check_yahoofinance_choose_path',
    python_callable = check_yahoofinance_choose,
    do_xcom_push = False,
    dag = dag
)

check_sginvestor_choose_path = BranchPythonOperator(
    task_id = 'check_sginvestor_choose_path',
    python_callable = check_sginvestor_choose,
    do_xcom_push = False,
    dag = dag
)

check_sginvestor_blog_choose_path = BranchPythonOperator(
    task_id = 'check_sginvestor_blog_choose_path',
    python_callable = check_sginvestor_blog_choose,
    do_xcom_push = False,
    dag = dag
)

check_businesstimes_choose_path = BranchPythonOperator(
    task_id = 'check_businesstimes_choose_path',
    python_callable = check_businesstimes_choose,
    do_xcom_push = False,
    dag = dag
)

############################
# Define Airflow Operators #
############################

# # Create table to store financial news
# create_table_financial_news = PostgresOperator (
#     task_id = 'create_table_financial_news',
#     dag = dag, 
#     postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
#     sql = '''
#         CREATE TABLE IF NOT EXISTS financial_news (
#         id SERIAL PRIMARY KEY,
#         ticker TEXT NOT NULL,
#         date DATE,
#         title TEXT NOT NULL,
#         link TEXT NOT NULL,
#         source TEXT,
#         comments TEXT
#         );
#         '''
#     )

#################################
# Airflow Operators             #
# (Initialisation - Historical) #
#################################

# Scraping yahoo finance news init
yahoofinance_scraping_init = PythonOperator(
    task_id = 'yahoofinance_scraping_data_init',
    python_callable = yahoofinance_scraping_data_init,
    dag = dag
)

# Scraping sg investor news init
sginvestor_scraping_init = PythonOperator(
    task_id = 'sginvestor_scraping_data_init',
    python_callable = sginvestor_scraping_data_init,
    dag = dag
)

# Scraping sg investor blog news init
sginvestor_blog_scraping_init = PythonOperator(
    task_id = 'sginvestor_blog_scraping_data_init',
    python_callable = sginvestor_blog_scraping_data_init,
    dag = dag
)

# Scraping business times news init
business_times_scraping_init = PythonOperator(
    task_id = 'business_times_scraping_data_init',
    python_callable = business_times_scraping_data_init,
    dag = dag
)

scrape_yahoofinance_init = DummyOperator(
    task_id = 'scrape_yahoofinance_init',
    dag = dag
)

scrape_sginvestor_init = DummyOperator(
    task_id = 'scrape_sginvestor_init',
    dag = dag
)

scrape_sginvestor_blog_init = DummyOperator(
    task_id = 'scrape_sginvestor_blog_init',
    dag = dag
)

scrape_businesstimes_init = DummyOperator(
    task_id = 'scrape_businesstimes_init',
    dag = dag
)

#################################
# Airflow Operators             #
# (Daily)                       #
#################################

# Scraping yahoo finance news daily
yahoofinance_scraping_daily = PythonOperator(
    task_id = 'yahoofinance_scraping_data_daily',
    python_callable = yahoofinance_scraping_data_daily,
    dag = dag
)

# Scraping sg investor news daily
sginvestor_scraping_daily = PythonOperator(
    task_id = 'sginvestor_scraping_data_daily',
    python_callable = sginvestor_scraping_data_daily,
    dag = dag
)

# Scraping sg investor blog news daily
sginvestor_blog_scraping_daily = PythonOperator(
    task_id = 'sginvestor_blog_scraping_data_daily',
    python_callable = sginvestor_blog_scraping_data_daily,
    dag = dag
)

# Scraping business times news daily
business_times_scraping_daily = PythonOperator(
    task_id = 'business_times_scraping_data_daily',
    python_callable = business_times_scraping_data_daily,
    dag = dag
)

scrape_yahoofinance_daily = DummyOperator(
    task_id = 'scrape_yahoofinance_daily',
    dag = dag
)

scrape_sginvestor_daily = DummyOperator(
    task_id = 'scrape_sginvestor_daily',
    dag = dag
)

scrape_sginvestor_blog_daily = DummyOperator(
    task_id = 'scrape_sginvestor_blog_daily',
    dag = dag
)

scrape_businesstimes_daily = DummyOperator(
    task_id = 'scrape_businesstimes_daily',
    dag = dag
)
############################
# Push to GCS From XCOMS #
############################

#################################
# (Initialisation - Historical) #
#################################

# Push yahoo finance news to cloud
yahoofinance_cloud_init = PythonOperator(
    task_id = 'push_yahoofinance_cloud_data_init',
    python_callable = push_yahoofinance_news_init,
    dag = dag
)

# Push sg investor news to cloud
sginvestor_cloud_init = PythonOperator(
    task_id = 'push_sginvestor_cloud_data_init',
    python_callable = push_sginvestor_news_init,
    dag = dag
)

# Push sg investor blog news to cloud
sginvestor_blog_cloud_init = PythonOperator(
    task_id = 'push_sginvestor_blog_cloud_data_init',
    python_callable = push_sginvestor_blog_news_init,
    dag = dag
)

# Push business times news to cloud
businesstimes_cloud_init = PythonOperator(
    task_id = 'push_businesstimes_cloud_data_init',
    python_callable = push_business_times_news_init,
    dag = dag
)

#################################
# Daily                         #
#################################

# Push yahoo finance news to cloud
yahoofinance_cloud_daily = PythonOperator(
    task_id = 'push_yahoofinance_cloud_data_daily',
    python_callable = push_yahoofinance_news_daily,
    dag = dag
)

# Push sg investor news to cloud
sginvestor_cloud_daily = PythonOperator(
    task_id = 'push_sginvestor_cloud_data_daily',
    python_callable = push_sginvestor_news_daily,
    dag = dag
)

# Push sg investor blog news to cloud
sginvestor_blog_cloud_daily = PythonOperator(
    task_id = 'push_sginvestor_blog_cloud_data_daily',
    python_callable = push_sginvestor_blog_news_daily,
    dag = dag
)

# Push business times news to cloud
businesstimes_cloud_daily = PythonOperator(
    task_id = 'push_businesstimes_cloud_data_daily',
    python_callable = push_business_times_news_daily,
    dag = dag
)

start_pipeline = DummyOperator(
    task_id = 'start_pipeline',
    dag = dag
)

# airflow tasks test dag_id (financial_news_scrape) tasK_id 2022-03-12
############################
# Define Tasks Hierarchy   #
############################

start_pipeline >> [check_yahoofinance_choose_path, check_sginvestor_choose_path, check_sginvestor_blog_choose_path, check_businesstimes_choose_path]

check_yahoofinance_choose_path >> [scrape_yahoofinance_init, scrape_yahoofinance_daily]
check_sginvestor_choose_path >> [scrape_sginvestor_init, scrape_sginvestor_daily]
check_sginvestor_blog_choose_path >> [scrape_sginvestor_blog_init, scrape_sginvestor_blog_daily]
check_businesstimes_choose_path >> [scrape_businesstimes_init, scrape_businesstimes_daily]

scrape_yahoofinance_init >> yahoofinance_scraping_init >> yahoofinance_cloud_init
scrape_sginvestor_init >> sginvestor_scraping_init >> sginvestor_cloud_init
scrape_sginvestor_blog_init >> sginvestor_blog_scraping_init >> sginvestor_blog_cloud_init
scrape_businesstimes_init >> business_times_scraping_init >> businesstimes_cloud_init

scrape_yahoofinance_daily >> yahoofinance_scraping_daily >> yahoofinance_cloud_daily
scrape_sginvestor_daily >> sginvestor_scraping_daily >> sginvestor_cloud_daily
scrape_sginvestor_blog_daily >> sginvestor_blog_scraping_daily >> sginvestor_blog_cloud_daily
scrape_businesstimes_daily >> business_times_scraping_daily >> businesstimes_cloud_daily