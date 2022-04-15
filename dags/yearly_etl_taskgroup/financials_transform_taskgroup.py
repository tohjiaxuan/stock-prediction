from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
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


def build_financials_transform_taskgroup(dag: DAG) -> TaskGroup:
    financials_transform_taskgroup = TaskGroup(group_id = 'financials_transform_tg')

    #####################
    # Transformation in #
    # Staging           #
    #####################


    def if_d_financials_exists(**kwargs):
        """ Checks if D_FINANCIALS is empty
        If D_FINANCIALS is empty, DAG will take the path to do transformation on historical data.
        If D_FINANCIALS is not empty, DAG will take the path to do transformation on the yearly (up-to-date) data.
        This is because historical data and yearly data each require different sets & techniques of transformation.

        Parameters
        ----------
        **kwargs: pass any keyword argument

        Returns
        -------
        taskid 
            Tells the DAG which path to take

        """
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            query = 'SELECT COUNT(*) FROM `stockprediction-344203.stock_prediction_datawarehouse.D_FINANCIALS`'
            df = bq_client.query(query).to_dataframe()
            df_length = df['f0_'].values[0]
            if (df_length != 0):
                return 'yearly_transformation_financials'
            else:
                return 'init_transformation_financials'
        except:
            return 'init_transformation_financials'

    # dummy operator to denote path to do transformation on historical data
    init_transformation_financials = DummyOperator(
        task_id = 'init_transformation_financials',
        dag = dag
    )

    # dummy operator to denote path to do transformation on yearly data
    yearly_transformation_financials = DummyOperator(
        task_id = 'yearly_transformation_financials',
        dag = dag
    )

    # BranchPythonOperator branches out the 2 paths that the DAG can take
    check_financials_choose_transform_path = BranchPythonOperator(
        task_id = 'check_financials_choose_transform_path',
        python_callable = if_d_financials_exists,
        do_xcom_push = False,
        dag = dag
    )

    
    ##################
    # Initialisation #
    ##################
    # Transforms historical data

    # Reformat Tables by unpivoting, removes duplicates
    reformat_netincome_init = BigQueryOperator(
        task_id = 'reformat_netincome_init',
        use_legacy_sql = False,
        sql = f'''
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_init` AS SELECT * FROM
        (SELECT DISTINCT ticker, year, netincome
        FROM `{PROJECT_ID}.{STAGING_DATASET}.netincome` 
        UNPIVOT
        (netincome FOR year
        IN (year2021 AS '2021', year2020 AS '2020', year2019 AS '2019', year2018 AS '2018', year2017 AS '2017')))
        ''',
        dag = dag
    )

    # Reformat Tables by unpivoting, removes duplicates
    reformat_assets_init = BigQueryOperator(
        task_id = 'reformat_assets_init',
        use_legacy_sql = False,
        sql = f'''
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_init` AS SELECT * FROM
        (SELECT DISTINCT ticker, year, assets
        FROM `{PROJECT_ID}.{STAGING_DATASET}.assets` 
        UNPIVOT
        (assets FOR year
        IN (year2021 AS '2021', year2020 AS '2020', year2019 AS '2019', year2018 AS '2018', year2017 AS '2017')))
        ''',
        dag = dag
    )

    # Reformat Tables by unpivoting, removes duplicates
    reformat_liab_init = BigQueryOperator(
        task_id = 'reformat_liab_init',
        use_legacy_sql = False,
        sql = f'''
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_init` AS SELECT * FROM
        (SELECT DISTINCT ticker, year, liability
        FROM `{PROJECT_ID}.{STAGING_DATASET}.liab` 
        UNPIVOT
        (liability FOR year
        IN (year2021 AS '2021', year2020 AS '2020', year2019 AS '2019', year2018 AS '2018', year2017 AS '2017')))
        ''',
        dag = dag
    )

    # Reformat Tables by unpivoting, removes duplicates
    reformat_equity_init = BigQueryOperator(
        task_id = 'reformat_equity_init',
        use_legacy_sql = False,
        sql = f'''
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_init` AS SELECT * FROM
        (SELECT DISTINCT ticker, year, equity
        FROM `{PROJECT_ID}.{STAGING_DATASET}.equity` 
        UNPIVOT
        (equity FOR year
        IN (year2021 AS '2021', year2020 AS '2020', year2019 AS '2019', year2018 AS '2018', year2017 AS '2017')))
        ''',
        dag = dag
    )

    # Reformat Tables by unpivoting, removes duplicates
    reformat_div_init = BigQueryOperator(
        task_id = 'reformat_div_init',
        use_legacy_sql = False,
        sql = f'''
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_init` AS SELECT * FROM
        (SELECT DISTINCT ticker, year, dividends
        FROM `{PROJECT_ID}.{STAGING_DATASET}.div` 
        UNPIVOT
        (dividends FOR year
        IN (year2021 AS '2021', year2020 AS '2020', year2019 AS '2019', year2018 AS '2018', year2017 AS '2017')))
        ''',
        dag = dag
    )

    # Join the tables containing each financial data type.
    join_financials = BigQueryOperator(
        task_id = 'join_financials',
        use_legacy_sql = False,
        sql = f'''
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.financials_join` 
            (
                ticker string not null,
                year string not null,
                netincome float64, 
                assets float64, 
                liability float64, 
                equity float64, 
                dividends float64
            )
            AS
            SELECT iale.ticker, iale.year, iale.netincome, iale.assets, iale.liability, iale.equity, d.dividends
            FROM 
            (SELECT ial.ticker, ial.year, ial.netincome, ial.assets, ial.liability, e.equity
            FROM 
            (SELECT ia.ticker, ia.year, ia.netincome, ia.assets, l.liability 
            FROM (SELECT i.ticker, i.year, i.netincome, a.assets FROM `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_init` i
                    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_init` a 
                            ON i.ticker = a.ticker AND i.year = a.year) AS ia 
                    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_init` l
                            ON ia.ticker = l.ticker AND ia.year = l.year) ial
                    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_init` e
                            ON ial.ticker = e.ticker AND ial.year = e.year) iale
                    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_init` d
                            ON iale.ticker = d.ticker AND iale.year = d.year
        ''',
        dag = dag
    )


    # Add financial ratio calculations
    add_financial_ratios = BigQueryOperator(
        task_id = 'add_financial_ratios',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.financials_with_ratios` 
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
                AS
                SELECT concat(ticker, '.SI') AS ticker, parse_timestamp("%Y-%m-%d", concat(year, '-12-31')) AS year, netincome, assets, liability, equity, dividends,
                (CASE WHEN netincome IS null THEN null
                WHEN netincome = 0 THEN 0
                WHEN assets IS null THEN null
                WHEN assets = 0 THEN null 
                ELSE netincome/assets END) AS ROA, 
                (CASE WHEN netincome IS null THEN null
                WHEN netincome = 0 THEN 0
                WHEN equity IS null THEN null
                WHEN equity = 0 THEN null 
                ELSE netincome/equity END) AS ROE,
                (CASE WHEN liability IS null THEN null
                WHEN liability = 0 THEN 0
                WHEN equity IS null THEN null
                WHEN equity = 0 THEN null 
                ELSE liability/equity END) AS debttoequity, 
                (CASE WHEN liability IS null THEN null
                WHEN assets IS null THEN null
                ELSE liability-assets END) AS networth,  
                FROM
                `{PROJECT_ID}.{STAGING_DATASET}.financials_join`
        ''',
        dag = dag
    )

    # Reformat financials_with_ratios table to format: ID (Key) | Year | Type (e.g. netincome, assets, roa or roe etc) | Value
    reformat_financial_ratios = BigQueryOperator(
        task_id = 'reformat_financial_ratios',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_financials_ratios` AS 
                SELECT concat(ticker, '-', EXTRACT(YEAR from year), '-', type) AS id,
                ticker, year, type, value FROM 
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
    # Transform yearly (up-to-date) data

    # Reformat Tables, removes duplicates
    reformat_netincome_yearly = BigQueryOperator(
        task_id = 'reformat_netincome_yearly',
        use_legacy_sql = False,
        sql = f'''
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_yearly` AS
            (SELECT DISTINCT ticker, CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING) AS year, prev_year_data AS netincome FROM `{PROJECT_ID}.{STAGING_DATASET}.netincome`)
        ''',
        dag = dag
    )

    # Reformat Tables, removes duplicates
    reformat_assets_yearly = BigQueryOperator(
        task_id = 'reformat_assets_yearly',
        use_legacy_sql = False,
        sql = f'''
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_yearly` AS
            (SELECT DISTINCT ticker, CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING) AS year, prev_year_data AS assets FROM `{PROJECT_ID}.{STAGING_DATASET}.assets`)
        ''',
        dag = dag
    )

    # Reformat Tables, removes duplicates
    reformat_liab_yearly = BigQueryOperator(
        task_id = 'reformat_liab_yearly',
        use_legacy_sql = False,
        sql = f'''
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_yearly` AS
            (SELECT DISTINCT ticker, CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING) AS year, prev_year_data AS liability FROM `{PROJECT_ID}.{STAGING_DATASET}.liab`)
        ''',
        dag = dag
    )

    # Reformat Tables, removes duplicates
    reformat_equity_yearly = BigQueryOperator(
        task_id = 'reformat_equity_yearly',
        use_legacy_sql = False,
        sql = f'''
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_yearly` AS
            (SELECT DISTINCT ticker, CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING) AS year, prev_year_data AS equity FROM `{PROJECT_ID}.{STAGING_DATASET}.equity`)
        ''',
        dag = dag
    )

    # Reformat Tables, removes duplicates
    reformat_div_yearly = BigQueryOperator(
        task_id = 'reformat_div_yearly',
        use_legacy_sql = False,
        sql = f'''
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_yearly` AS
            (SELECT DISTINCT ticker, CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING) AS year, prev_year_data AS dividends FROM `{PROJECT_ID}.{STAGING_DATASET}.div`)
        ''',
        dag = dag
    )

    # Join the tables containing each financial data type.
    join_financials_yearly = BigQueryOperator(
        task_id = 'join_financials_yearly',
        use_legacy_sql = False,
        sql = f'''
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.financials_join_yearly` 
            (
                ticker string not null,
                year string not null,
                netincome float64, 
                assets float64, 
                liability float64, 
                equity float64, 
                dividends float64
            )
            AS
            SELECT iale.ticker, iale.year, iale.netincome, iale.assets, iale.liability, iale.equity, d.dividends
            FROM 
            (SELECT ial.ticker, ial.year, ial.netincome, ial.assets, ial.liability, e.equity
            FROM 
            (SELECT ia.ticker, ia.year, ia.netincome, ia.assets, l.liability 
            FROM (SELECT i.ticker, i.year, i.netincome, a.assets FROM `{PROJECT_ID}.{STAGING_DATASET}.reformat_netincome_yearly` i
                    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.reformat_assets_yearly` a 
                            ON i.ticker = a.ticker AND i.year = a.year) AS ia 
                    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.reformat_liab_yearly` l
                            ON ia.ticker = l.ticker AND ia.year = l.year) ial
                    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.reformat_equity_yearly` e
                            ON ial.ticker = e.ticker AND ial.year = e.year) iale
                    LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.reformat_div_yearly` d
                            ON iale.ticker = d.ticker AND iale.year = d.year
        ''',
        dag = dag
    )

    # Add financial ratio calculations
    add_financial_ratios_yearly = BigQueryOperator(
        task_id = 'add_financial_ratios_yearly',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.financials_with_ratios_yearly` 
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
                AS
                SELECT concat(ticker, '.SI') AS ticker, parse_timestamp("%Y-%m-%d", concat(year, '-12-31')) AS year, netincome, assets, liability, equity, dividends,
                (CASE WHEN netincome IS null THEN null
                WHEN netincome = 0 THEN 0
                WHEN assets IS null THEN null
                WHEN assets = 0 THEN null 
                ELSE netincome/assets END) AS ROA, 
                (CASE WHEN netincome IS null THEN null
                WHEN netincome = 0 THEN 0
                WHEN equity IS null THEN null
                WHEN equity = 0 THEN null 
                ELSE netincome/equity END) AS ROE,
                (CASE WHEN liability IS null THEN null
                WHEN liability = 0 THEN 0
                WHEN equity IS null THEN null
                WHEN equity = 0 THEN null 
                ELSE liability/equity END) AS debttoequity, 
                (CASE WHEN liability IS null THEN null
                WHEN assets IS null THEN null
                ELSE liability-assets END) AS networth,  
                FROM
                `{PROJECT_ID}.{STAGING_DATASET}.financials_join_yearly`
        ''',
        dag = dag
    )

    # Reformat financials_with_ratios_yearly table to format: ID (Key) | Year | Type (e.g. netincome, assets, roa or roe etc) | Value
    reformat_financial_ratios_yearly = BigQueryOperator(
        task_id = 'reformat_financial_ratios_yearly',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.reformat_financials_ratios` AS 
                SELECT CONCAT(ticker, '-', EXTRACT(YEAR from year), '-', type) AS id,
                ticker, year, type, value FROM 
                (SELECT distinct ticker, year, type, value
                FROM `{PROJECT_ID}.{STAGING_DATASET}.financials_with_ratios_yearly` 
                UNPIVOT
                (value FOR type
                IN (netincome, assets, liability, equity, yearlydividends, roa, roe, debttoequity, networth)))
        ''',
        dag = dag
    )

    # Add a unique identifier ID (key)
    inflation_key = BigQueryOperator(
        task_id = 'inflation_key',
        use_legacy_sql = False,
        sql = f'''
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{STAGING_DATASET}.inflation_key` AS
                SELECT CONCAT(EXTRACT(YEAR from temp.year), '-inflation') AS id, year, CAST(inflation AS FLOAT64) AS inflation
                FROM
                (SELECT parse_timestamp("%Y-%m-%d", concat(year, '-12-31')) AS year, inflation FROM `{PROJECT_ID}.{STAGING_DATASET}.inflation`) AS temp
        ''',
        dag = dag
    )

    # kickstart transformation
    start_transformation = DummyOperator(
        task_id = 'start_transformation',
        dag = dag
    )

    # end of transformation
    end_transformation = BashOperator(
        task_id="end_transformation",
        bash_command="echo end_transformation",
        trigger_rule="one_success",
        dag=dag
    )
    
    # TASK DEPENDENCIES

    start_transformation >> inflation_key >> check_financials_choose_transform_path
    check_financials_choose_transform_path >> [init_transformation_financials, yearly_transformation_financials]

    init_transformation_financials >> [reformat_netincome_init, reformat_assets_init, reformat_liab_init, reformat_equity_init, reformat_div_init] >> join_financials >> add_financial_ratios >> reformat_financial_ratios
    yearly_transformation_financials >> [reformat_netincome_yearly, reformat_assets_yearly, reformat_liab_yearly, reformat_equity_yearly, reformat_div_yearly] >> join_financials_yearly >> add_financial_ratios_yearly >> reformat_financial_ratios_yearly
    
    [reformat_financial_ratios, reformat_financial_ratios_yearly] >> end_transformation

    
    return financials_transform_taskgroup