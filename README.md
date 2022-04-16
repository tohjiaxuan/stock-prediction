# ETL & DWH Project
The main aim of this project is to create a ETL pipeline using Apache Airlow and a data warehouse on BigQuery for downstream applications such as Stock Analysis (Prediction) & Sentiment Analysis. 

Deliverables for this project includes:
1. 3 DAGs created using Python
2. GCS bucket in Google Cloud Platform
3. Staging tables hosted in BigQuery
4. PostgreSQL database used to conduct transformations in times of failure
5. Data Warehouse hosted in BigQuery

# File Directory to be followed in VM
```
├── dags
│   └── daily_dag.py
│   └── extract_taskgroup.py
│   └── gcs_taskgroup.py
│   └── load_taskgroup.py
│   └── daily_postgres_taskgroup.py
│   └── stage_taskgroup.py
│   └── transform_taskgroup.py
│   └── daily_financial_news_dag.py
│   └── extract_news_taskgroup.py
│   └── gcs_news_taskgroup.py
│   └── load_news_taskgroup.py
│   └── postgres_news_taskgroup.py
│   └── stage_news_taskgroup.py
│   └── transform_news_taskgroup.py
│   └── sql
│       ├── D_commodities.sql
│       ├── D_exchange_rate.sql
│       ├── D_financials.sql
│       ├── D_inflation.sql
│       ├── D_interest_rate.sql
│       ├── F_news.sql
│       ├── F_stock.sql
│   └── financials_dwh_schemas.py
│   └── financials_extract_taskgroup.py
│   └── financials_gcs_taskgroup.py
│   └── financials_load_taskgroup.py
│   └── financials_postgres_taskgroup.py
│   └── financials_schema_taskgroup.py
│   └── financials_stage_taskgroup.py
│   └── financials_transform_taskgroup.py
│   └── yearly_dag_etl.py
│   └── sti.csv
```

# General Configurations before running DAGs
1. Set up Apache Airflow as per normal (create your own account etc)
2. Make use of the requirements.txt file to pip install all the packages that are required other than Python
3. Head to `airflow.cfg` and change `enable_xcom_pickling = True`
4. Create GCS bucket on Google Cloud Platform (https://console.cloud.google.com/)
    1. Set location to asia-southeast1 (Singapore)
    2. Turn off object versioning, do not choose a retention policy and set encryption type to be google-managed key
    3. Set public access to be not public
    4. Developer should set the permissions by giving corresponding users the right access and role
    5. Download the Google ServiceKey (in JSON)

3. In the Airflow webserver, head to `Admin` > `Connections`. 
    1. Edit the `google_cloud_default` Conn Id by pasting the Google ServiceKey (in JSON format) in the `Keyfile JSON` field. 
    2. Set up PostgreSQL if you have not done so and then create a `postgres_local` Conn Id with your credentials - set `Host` and `Port` as `localhost` and `5432` respectively. More details can be found below under "Creating connection(s) on Airflow".

4. To ensure that the processing unit is closest to you, after setting up the DWH, enter BigQuery console. In the editor head to `MORE` > `Query Settings` > `Data location` and select `asia-southeast1 (Singapore)` and save the query setting

# Configurations to be edited and set in `airflow.cfg`
### To configure the sending of emails on task failure:
1. Generate Google App password:
    1. Visit the App Passwords page (https://security.google.com/settings/security/apppasswords)
    2. Select App as `airflow` and select `Generate`.
    3. Copy the generated 16 digit password. 
2. Edit `airflow.cfg` file with the following:
    1. smtp_host = smtp.googlemail.com
    2. smtp_user = YOUR_EMAIL_ADDRESS
    3. smtp_password = 16_DIGIT_APP_PASSWORD
    4. smtp_mail_from = YOUR_EMAIL_ADDRESS
3. Edit main DAG file:
    1. Input desired email in the `email:` field in `default_args`

### To enable parallel processing
1. Edit `airflow.cfg` file with the following: 
    1. Change the executor: replace with `executor = LocalExecutor`
    2. Change SqlAlchemy connection: replace with `sql_alchemy_conn = postgresql+psycopg2://postgres_local:airflow@localhost/postgres_db`
2. Run the following commands within the terminal:
Note: you will first be prompted to create a new user. Create a new user with your own credentials. Do make sure your `postgres_local` Conn Id is still present. Otherwise, recreate that Conn Id. 
    1. Run `airflow db init`
    2. Re-run the airflow webserver and scheduler. 

# Creating connection(s) on Airflow
### Connect to PostGres (After the PostGres Database has been created)
1. Add a new conn_id
    1. In Airflow, go to Admin > Connections > Select `+` to add connection to PostGres
2. Use the following parameters:
    1. Connection Id: postgres_local
    2. Connection Type: Postgres
    3. Description: Airflow connection to PostGres
    4. Host: localhost
    5. Schema: postgres_db
    6. Login: postgres_local
    7. Port: 5432
    
# Running of DAGs
1. Turn on all the DAGs but do not trigger them yet 
2. Trigger `yearly_dag_etl` and `daily_financial_news_dag` (order does not matter)

**Note: `daily_dag` does not require the user to manually trigger it. It will be triggered by `yearly_dag_etl` upon completion via the `TriggerDagRunOperator`.
