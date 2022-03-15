from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import numpy as np
import time
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, InvalidArgumentException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from xvfbwrapper import Xvfb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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

# Function for (1) scraping yahoo finance news and (2) storing into psql table
def yahoofinance_scraping_data(**kwargs):

    vdisplay = Xvfb()
    vdisplay.start()

    # Chrome driver
    # Select custom Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument('--headless') 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(ChromeDriverManager().install(), options = options)

    # scrape news for each ticker
    start = 'https://sg.finance.yahoo.com/quote/'
    end = '/news'
    df_final = pd.DataFrame(columns =['Ticker', 'Title', 'Date', 'Link'])
    
    for ticker in tickers_df['New Symbol']:
        #   print("Starting collection for:", i)
        try: 
            link = start + ticker + end
            driver.get(link)
            time.sleep(10)
            print('-----------------')
            print(ticker)

            # scrolling
            height = driver.execute_script("return document.documentElement.scrollHeight")
            for i in range(height):
                driver.execute_script('window.scrollBy(0,20)') # scroll by 20 on each iteration
                height = driver.execute_script("return document.documentElement.scrollHeight") 
                # reset height to the new height after scroll-triggered elements have been loaded. 
            # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li[*]/div/div/div[*]/h3/a')))
            test = driver.find_elements(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li[*]/div/div/div[*]/h3/a')
            for i in range(len(test)):
                
                link = ''
                date = ''
                title = ''
                index = i+1

                # get links
                elems_links = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                link =elems_links.get_attribute("href")
                #links = [my_elem.get_attribute("href") for my_elem in elems_links]

                # get titles
                elems_titles = driver.find_element(By.XPATH, '//*[contains(@id, "NewsStream-0-Stream")]/ul/li['+str(index)+']/div/div/div[*]/h3/a')
                title =elems_titles.text
                #titles = [my_elem.text for my_elem in elems_titles]

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

    df_final['Title'] = df_final['Title'].astype(str).str.replace("'", "")
    df_final['Title'] = df_final['Title'].astype(str).str.replace('"', '')

    # remove row if it is ad
    df_final.replace("", np.nan, inplace=True)
    df_final.dropna(subset=['Date'], inplace = True)
    df_final['Source'] = 'Yahoo Finance News'
    df_final['Comments'] = ''


    # Clean up Updated SG Time information (put in terms of date)
    cleaned_time = []
    current_time = datetime.today()
    for t in df_final['Date']:
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
    df_final.reset_index(drop=True)  
    print('clean successfully')   

    for i in range(len(df_final)):

        print('this is query')
        row = df_final.loc[i,:]
        print(row)

        query = f'''
        INSERT INTO yahoofinance (ticker, date, title, link, source, comments)
        VALUES ('{row["Ticker"]}', DATE '{row["Date"]}', '{row["Title"]}', '{row["Link"]}', '{row["Source"]}', '{row["Comments"]}');
        '''
        print(query)
        execute_query_with_hook(query)
    print('queries successful')

############################
# Define Airflow Operators #
############################

# Create table to store yahoo finance news
create_table_yahoofinance = PostgresOperator (
    task_id = 'create_table_yahoofinance',
    dag = dag, 
    postgres_conn_id="postgres_local", #inline with our airflow configuration setting (the connection id)
    sql = '''
        CREATE TABLE IF NOT EXISTS yahoofinance (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
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


# airflow tasks test dag_id tasK_id 2022-03-12
############################
# Define Tasks Hierarchy   #
############################

create_table_yahoofinance >> yahoofinance_scraping
