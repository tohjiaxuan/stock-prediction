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


def build_financials_transform_taskgroup(dag: DAG) -> TaskGroup:
    financials_transform_taskgroup = TaskGroup(group_id = 'financials_transform_tg')

    
    #####################
    # Transformation in #
    # Staging           #
    #####################

    # Check if tables in datawarehouse are empty. 
    # If empty, do the transformation on initialisation (historical) data. 
    # Otherwise, do the transformation on the up-to-date yearly data. 

    def if_d_financials_exists(**kwargs):
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            query = 'select COUNT(*) from `stockprediction-344203.stock_prediction_datawarehouse.D_FINANCIALS`'
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return 'yearly_transformation_financials'
            else:
                return 'init_transformation_financials'
        except:
            return 'init_transformation_financials'

    init_transformation_financials = DummyOperator(
        task_id = 'init_transformation_financials',
        dag = dag
    )

    yearly_transformation_financials = DummyOperator(
        task_id = 'yearly_transformation_financials',
        dag = dag
    )

    # Check which path should taken - initialisation or yearly.
    check_financials_choose_transform_path = BranchPythonOperator(
        task_id = 'check_financials_choose_transform_path',
        python_callable = if_d_financials_exists,
        do_xcom_push = False,
        dag = dag
    )

    
    ##################
    # Initialisation #
    ##################

    # Reformat Tables by unpivoting, removes duplicates
    reformat_netincome_init = BigQueryOperator(
        task_id = 'reformat_netincome_init',
        use_legacy_sql = False,
        sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_init` as select * from
        (SELECT distinct ticker, year, netincome
        FROM `{PROJECT_ID}.{STAGING_DATASET}.netincome` 
        UNPIVOT
        (netincome FOR year
        IN (year2021 as '2021', year2020 as '2020', year2019 as '2019', year2018 as '2018', year2017 as '2017')))
        ''',
        dag = dag
    )

    # Reformat Tables by unpivoting, removes duplicates
    reformat_assets_init = BigQueryOperator(
        task_id = 'reformat_assets_init',
        use_legacy_sql = False,
        sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_init` as select * from
        (SELECT distinct ticker, year, assets
        FROM `{PROJECT_ID}.{STAGING_DATASET}.assets` 
        UNPIVOT
        (assets FOR year
        IN (year2021 as '2021', year2020 as '2020', year2019 as '2019', year2018 as '2018', year2017 as '2017')))
        ''',
        dag = dag
    )

    # Reformat Tables by unpivoting, removes duplicates
    reformat_liab_init = BigQueryOperator(
        task_id = 'reformat_liab_init',
        use_legacy_sql = False,
        sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_init` as select * from
        (SELECT distinct ticker, year, liability
        FROM `{PROJECT_ID}.{STAGING_DATASET}.liab` 
        UNPIVOT
        (liability FOR year
        IN (year2021 as '2021', year2020 as '2020', year2019 as '2019', year2018 as '2018', year2017 as '2017')))
        ''',
        dag = dag
    )

    # Reformat Tables by unpivoting, removes duplicates
    reformat_equity_init = BigQueryOperator(
        task_id = 'reformat_equity_init',
        use_legacy_sql = False,
        sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_init` as select * from
        (SELECT distinct ticker, year, equity
        FROM `{PROJECT_ID}.{STAGING_DATASET}.equity` 
        UNPIVOT
        (equity FOR year
        IN (year2021 as '2021', year2020 as '2020', year2019 as '2019', year2018 as '2018', year2017 as '2017')))
        ''',
        dag = dag
    )

    # Reformat Tables by unpivoting, removes duplicates
    reformat_div_init = BigQueryOperator(
        task_id = 'reformat_div_init',
        use_legacy_sql = False,
        sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_init` as select * from
        (SELECT distinct ticker, year, dividends
        FROM `{PROJECT_ID}.{STAGING_DATASET}.div` 
        UNPIVOT
        (dividends FOR year
        IN (year2021 as '2021', year2020 as '2020', year2019 as '2019', year2018 as '2018', year2017 as '2017')))
        ''',
        dag = dag
    )

    # join the tables containing each financial data type.
    join_financials = BigQueryOperator(
        task_id = 'join_financials',
        use_legacy_sql = False,
        sql = f'''
            create or replace table `{PROJECT_ID}.{STAGING_DATASET}.financials_join` 
            (
                ticker string not null,
                year string not null,
                netincome float64, 
                assets float64, 
                liability float64, 
                equity float64, 
                dividends float64
            )
            as
            select iale.ticker, iale.year, iale.netincome, iale.assets, iale.liability, iale.equity, d.dividends
            from 
            (select ial.ticker, ial.year, ial.netincome, ial.assets, ial.liability, e.equity
            from 
            (select ia.ticker, ia.year, ia.netincome, ia.assets, l.liability 
            from (SELECT i.ticker, i.year, i.netincome, a.assets FROM `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_init` i
                    left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_init` a 
                            on i.ticker = a.ticker and i.year = a.year) as ia 
                    left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_init` l
                            on ia.ticker = l.ticker and ia.year = l.year) ial
                    left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_init` e
                            on ial.ticker = e.ticker and ial.year = e.year) iale
                    left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_init` d
                            on iale.ticker = d.ticker and iale.year = d.year
        ''',
        dag = dag
    )


    # Add financial ratio calculations
    add_financial_ratios = BigQueryOperator(
        task_id = 'add_financial_ratios',
        use_legacy_sql = False,
        sql = f'''
                create or replace table `{PROJECT_ID}.{STAGING_DATASET}.financials_with_ratios` 
                (
                    ticker string not null,
                    year timestamp not null,
                    netincome float64, 
                    assets float64, 
                    liability float64, 
                    equity float64, 
                    yearlydividends float64,
                    ROA float64,
                    ROE float64,
                    debttoequity float64,
                    networth float64
                )
                as
                select concat(ticker, '.SI') as ticker, parse_timestamp("%Y-%m-%d", concat(year, '-12-31')) as year, netincome, assets, liability, equity, dividends,
                (case when netincome is null then null
                when netincome = 0 then 0
                when assets is null then null
                when assets = 0 then null 
                else netincome/assets end) as ROA, 

                (case when netincome is null then null
                when netincome = 0 then 0
                when equity is null then null
                when equity = 0 then null 
                else netincome/equity end) as ROE,

                (case when liability is null then null
                when liability = 0 then 0
                when equity is null then null
                when equity = 0 then null 
                else liability/equity end) as debttoequity, 

                (case when liability is null then null
                when assets is null then null
                else liability-assets end) as networth,  
                from
                `{PROJECT_ID}.{STAGING_DATASET}.financials_join`
        ''',
        dag = dag
    )

    # Reformat financials_with_ratios table to format: ID (Key) | Year | Type (e.g. netincome, assets, roa or roe etc) | Value
    # Add a unique identifier ID to this table too (i.e. column ID)
    reformat_financial_ratios = BigQueryOperator(
        task_id = 'reformat_financial_ratios',
        use_legacy_sql = False,
        sql = f'''
                create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_financials_ratios` as 
                SELECT concat(ticker, '-', EXTRACT(YEAR from year), '-', type) as id,
                ticker, year, type, value from 
                (SELECT distinct ticker, year, type, value
                FROM `{PROJECT_ID}.{STAGING_DATASET}.financials_with_ratios` 
                UNPIVOT
                (value FOR type
                IN (netincome, assets, liability, equity, yearlydividends, roa, roe, debttoequity, networth)))
        ''',
        dag = dag
    )

    

    ##################
    # Yearly         #
    ##################
    # Update with up-to-date data

    # Reformat Tables, removes duplicates
    reformat_netincome_yearly = BigQueryOperator(
        task_id = 'reformat_netincome_yearly',
        use_legacy_sql = False,
        sql = f'''
            create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_yearly` as
            (SELECT distinct ticker, CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING) as year, year2022 as netincome FROM `{PROJECT_ID}.{STAGING_DATASET}.netincome`)
        ''',
        dag = dag
    )

    # Reformat Tables, removes duplicates
    reformat_assets_yearly = BigQueryOperator(
        task_id = 'reformat_assets_yearly',
        use_legacy_sql = False,
        sql = f'''
            create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_yearly` as
            (SELECT distinct ticker, CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING) as year, year2022 as assets FROM `{PROJECT_ID}.{STAGING_DATASET}.assets`)
        ''',
        dag = dag
    )

    # Reformat Tables, removes duplicates
    reformat_liab_yearly = BigQueryOperator(
        task_id = 'reformat_liab_yearly',
        use_legacy_sql = False,
        sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_yearly` as
            (SELECT distinct ticker, CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING) as year, year2022 as liability FROM `{PROJECT_ID}.{STAGING_DATASET}.liab`)
        ''',
        dag = dag
    )

    # Reformat Tables, removes duplicates
    reformat_equity_yearly = BigQueryOperator(
        task_id = 'reformat_equity_yearly',
        use_legacy_sql = False,
        sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_yearly` as
            (SELECT distinct ticker, CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING) as year, year2022 as equity FROM `{PROJECT_ID}.{STAGING_DATASET}.equity`)
        ''',
        dag = dag
    )

    # Reformat Tables, removes duplicates
    reformat_div_yearly = BigQueryOperator(
        task_id = 'reformat_div_yearly',
        use_legacy_sql = False,
        sql = f'''
        create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_yearly` as
            (SELECT distinct ticker, CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING) as year, year2022 as dividends FROM `{PROJECT_ID}.{STAGING_DATASET}.div`)
        ''',
        dag = dag
    )

    # join the tables containing each financial data type.
    join_financials_yearly = BigQueryOperator(
        task_id = 'join_financials_yearly',
        use_legacy_sql = False,
        sql = f'''
            create or replace table `{PROJECT_ID}.{STAGING_DATASET}.financials_join_yearly` 
            (
                ticker string not null,
                year string not null,
                netincome float64, 
                assets float64, 
                liability float64, 
                equity float64, 
                dividends float64
            )
            as
            select iale.ticker, iale.year, iale.netincome, iale.assets, iale.liability, iale.equity, d.dividends
            from 
            (select ial.ticker, ial.year, ial.netincome, ial.assets, ial.liability, e.equity
            from 
            (select ia.ticker, ia.year, ia.netincome, ia.assets, l.liability 
            from (SELECT i.ticker, i.year, i.netincome, a.assets FROM `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_yearly` i
                    left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_yearly` a 
                            on i.ticker = a.ticker and i.year = a.year) as ia 
                    left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_yearly` l
                            on ia.ticker = l.ticker and ia.year = l.year) ial
                    left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_yearly` e
                            on ial.ticker = e.ticker and ial.year = e.year) iale
                    left join `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_yearly` d
                            on iale.ticker = d.ticker and iale.year = d.year
        ''',
        dag = dag
    )

    # Add financial ratio calculations
    add_financial_ratios_yearly = BigQueryOperator(
        task_id = 'add_financial_ratios_yearly',
        use_legacy_sql = False,
        sql = f'''
                create or replace table `{PROJECT_ID}.{STAGING_DATASET}.financials_with_ratios_yearly` 
                (
                    ticker string not null,
                    year timestamp not null,
                    netincome float64, 
                    assets float64, 
                    liability float64, 
                    equity float64, 
                    yearlydividends float64,
                    ROA float64,
                    ROE float64,
                    debttoequity float64,
                    networth float64
                )
                as
                select concat(ticker, '.SI') as ticker, parse_timestamp("%Y-%m-%d", concat(year, '-12-31')) as year, netincome, assets, liability, equity, dividends,
                (case when netincome is null then null
                when netincome = 0 then 0
                when assets is null then null
                when assets = 0 then null 
                else netincome/assets end) as ROA, 

                (case when netincome is null then null
                when netincome = 0 then 0
                when equity is null then null
                when equity = 0 then null 
                else netincome/equity end) as ROE,

                (case when liability is null then null
                when liability = 0 then 0
                when equity is null then null
                when equity = 0 then null 
                else liability/equity end) as debttoequity, 

                (case when liability is null then null
                when assets is null then null
                else liability-assets end) as networth,  
                from
                `{PROJECT_ID}.{STAGING_DATASET}.financials_join_yearly`
        ''',
        dag = dag
    )

    # Reformat financials_with_ratios_yearly table to format: ID (Key) | Year | Type (e.g. netincome, assets, roa or roe etc) | Value
    reformat_financial_ratios_yearly = BigQueryOperator(
        task_id = 'reformat_financial_ratios_yearly',
        use_legacy_sql = False,
        sql = f'''
                create or replace table `{PROJECT_ID}.{STAGING_DATASET}.reformat_financials_ratios` as 
                SELECT concat(ticker, '-', EXTRACT(YEAR from year), '-', type) as id,
                ticker, year, type, value from 
                (SELECT distinct ticker, year, type, value
                FROM `{PROJECT_ID}.{STAGING_DATASET}.financials_with_ratios_yearly` 
                UNPIVOT
                (value FOR type
                IN (netincome, assets, liability, equity, yearlydividends, roa, roe, debttoequity, networth)))
        ''',
        dag = dag
    )

    # Add a unique identifier ID (key) to the inflation_yearly table (i.e. column ID)
    inflation_key = BigQueryOperator(
        task_id = 'inflation_key',
        use_legacy_sql = False,
        sql = f'''
                create or replace table `{PROJECT_ID}.{STAGING_DATASET}.inflation_key` as
                select concat(EXTRACT(YEAR from temp.year), '-inflation') as id, year, CAST(inflation AS FLOAT64) as inflation
                from
                (SELECT parse_timestamp("%Y-%m-%d", concat(year, '-12-31')) as year, inflation FROM `{PROJECT_ID}.{STAGING_DATASET}.inflation`) as temp

        ''',
        dag = dag
    )


    start_transformation = DummyOperator(
        task_id = 'start_transformation',
        dag = dag
    )

    end_transformation = BashOperator(
        task_id="end_transformation",
        bash_command="echo end_transformation",
        trigger_rule="one_success",
        dag=dag
    )

    
    '''
    def force_fail():
        raise AirflowException("This error is to test the Postgres task!")

    # for testing purposes: insert this task after end_transformation, i.e. end_transformation >> force_fail
    # this simulates the event that the transformation task group fails, following which, the Postgres Transformation (on-premise) task group will be triggered to perform the transformation instead.
    force_fail = PythonOperator(
        task_id = 'force_fail',
        python_callable = force_fail
    )

    '''
    
    # TASK DEPENDENCIES
    start_transformation >> inflation_key >> check_financials_choose_transform_path
    check_financials_choose_transform_path >> [init_transformation_financials, yearly_transformation_financials]

    init_transformation_financials >> [reformat_netincome_init, reformat_assets_init, reformat_liab_init, reformat_equity_init, reformat_div_init] >> join_financials >> add_financial_ratios >> reformat_financial_ratios
    yearly_transformation_financials >> [reformat_netincome_yearly, reformat_assets_yearly, reformat_liab_yearly, reformat_equity_yearly, reformat_div_yearly] >> join_financials_yearly >> add_financial_ratios_yearly >> reformat_financial_ratios_yearly
    
    [reformat_financial_ratios, reformat_financial_ratios_yearly] >> end_transformation

    
    return financials_transform_taskgroup