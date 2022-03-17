from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import numpy as np
import time
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, InvalidArgumentException, WebDriverException, NoSuchElementException
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

# Helper function to clean the dates and titles
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
                
            #last_update = last_update.strftime('%d/%m/%Y %H:%M %p') # 27/02/2022 09:05 AM
            last_update = last_update.strftime('%Y/%m/%d') 
            cleaned_time.append(last_update)
    df_final['Date'] = cleaned_time   
    df_final.reset_index(drop=True, inplace = True)  
    df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
    df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')
    print('clean successfully')  
    return df_final

# Helper function to load query
def load_query(df_final):
    for i in range(len(df_final)):
        print('this is query')
        row = df_final.loc[i,:]
        print(row)
        query = f'''
        INSERT INTO financial_news (ticker, date, title, link, source, comments)
        VALUES ('{row["Ticker"]}', NULLIF('{row["Date"]}','')::DATE, '{row["Title"]}', '{row["Link"]}', '{row["Source"]}', '{row["Comments"]}');
        '''
        print(query)
        execute_query_with_hook(query)
    print('queries successful')

# Function for (1) scraping yahoo finance news and (2) storing into psql table
def yahoofinance_scraping_data(**kwargs):
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
                except NoSuchElementException as e:
                    pass
                finally: 
                    row = [ticker, title, date, link]
                    df_final.loc[len(df_final)] = row
        except TimeoutException as e:
            pass
        except WebDriverException as e:
            print('webdriver exception')
    print('scrape successfully')
    driver.quit()

    df_final = clean_df(df_final)

    # remove row if it is ad
    df_final.replace("", np.nan, inplace=True)
    df_final.dropna(subset=['Date'], inplace = True)
    df_final['Source'] = 'Yahoo Finance News'
    df_final['Comments'] = ''
    df_final.reset_index(drop=True, inplace = True) 

    load_query(df_final)


def sginvestor_scraping_data(**kwargs):
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

    load_query(df_final)

def sginvestor_blog_scraping_data(**kwargs):
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
    df_final = clean_df(df_final)
    df_final.head()

    load_query(df_final)        

############################
# Define Airflow Operators #
############################

# Create table to store financial news
create_table_financial_news = PostgresOperator (
    task_id = 'create_table_financial_news',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS financial_news (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        date DATE,
        title TEXT NOT NULL,
        link TEXT NOT NULL,
        source TEXT,
        comments TEXT
        );
        '''
    )

# Scraping yahoo finance news
yahoofinance_scraping = PythonOperator(
    task_id = 'yahoofinance_scraping_data',
    python_callable = yahoofinance_scraping_data,
    dag = dag
)

# Scraping sg investor news
sginvestor_scraping = PythonOperator(
    task_id = 'sginvestor_scraping_data',
    python_callable = sginvestor_scraping_data,
    dag = dag
)

# Scraping sg investor blog news
sginvestor_blog_scraping = PythonOperator(
    task_id = 'sginvestor_blog_scraping_data',
    python_callable = sginvestor_blog_scraping_data,
    dag = dag
)

# airflow tasks test dag_id tasK_id 2022-03-12
############################
# Define Tasks Hierarchy   #
############################

create_table_financial_news >> [yahoofinance_scraping, sginvestor_scraping, sginvestor_blog_scraping]
