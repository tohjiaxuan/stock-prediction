# ETL & DWH Project
The main aim of this project is to create a ETL pipeline using Apache Airlow and a data warehouse on BigQuery for downstream applicatinos such as Stock Analysis (Prediction) & Sentiment Analysis. 

Deliverables for this project includes:
1. 3 DAGs created using Python
2. GCS bucket in Google Cloud Platform
3. Staging tables hosted in BigQuery
4. PostgreSQL database used to conduct transformations in times of failure
5. Data Warehouse hosted in BigQuery

# Project Directory
```
├── Experimental Code
│   └── ...
├── dags
│   └── archive
│       ├── ...
│   └── daily_etl_taskgroup
│       ├── daily_dag.py
│       ├── extract_taskgroup.py
│       ├── gcs_taskgroup.py
│       ├── load_taskgroup.py
│       ├── daily_postgres_taskgroup.py
│       ├── stage_taskgroup.py
│       ├── transform_taskgroup.py
│   └── news_etl_taskgroup
│       ├── daily_financial_news.py
│       ├── extract_taskgroup.py
│       ├── gcs_taskgroup.py
│       ├── load_taskgroup.py
│       ├── postgres_taskgroup.py
│       ├── stage_taskgroup.py
│       ├── transform_taskgroup.py
│   └── sql
│       ├── D_commodities.sql
│       ├── D_exchange_rate.sql
│       ├── D_financials.sql
│       ├── D_inflation.sql
│       ├── D_interest_rate.sql
│       ├── F_news.sql
│       ├── F_stock.sql
│   └── yearly_etl_taskgroup
│       ├── financials_dwh_schemas.py
│       ├── financials_extract_taskgroup.py
│       ├── financials_gcs_taskgroup.py
│       ├── financials_load_taskgroup.py
│       ├── financials_postgres_taskgroup.py
│       ├── financials_schema_taskgroup.py
│       ├── financials_stage_taskgroup.py
│       ├── financials_transform_taskgroup.py
│       ├── yearly_dag_etl.py
│   └── sti.csv
```

# General Configurations needed
1. Head to `airflow.cfg` and change `enable_xcom_pickling = True`
2. Create GCS bucket on Google Cloud Platform
    1. Set location to asia-southeast1 (Singapore)
    2. Turn off object versioning, do not choose a retention policy and set encryption type to be google-managed key
    3. Set public access to be not public
    4. Developer should set the permissions by giving corresponding users the right access and role

3. In the Airflow webserver, head to `Admin` > `Connections`. 
    1. Edit the `google_cloud_default` Conn Id by pasting the Google ServiceKey (in JSON format) in the `Keyfile JSON` field. 
    2. Set up PostgreSQL if you have not done so and then create a `postgres_local` Conn Id with your credentials - set `Host` and `Port` as `localhost` and `5432` respectively. 

# Steps to configure the sending of emails on task failure
### Generate Google App password: 

a. Visit the App Passwords page (https://security.google.com/settings/security/apppasswords)

b. Select App as `airflow` and select `Generate`. 

c. Copy the generated 16 digit password. 

### Edit `airflow.cfg` file with the following:

smtp_host = smtp.googlemail.com

smtp_user = YOUR_EMAIL_ADDRESS

smtp_password = 16_DIGIT_APP_PASSWORD

smtp_mail_from = YOUR_EMAIL_ADDRESS

### Edit dag file:

Input desired email in the `email:` field in `default_args`

# Enabling Parallel Processing
### Edit `airflow.cfg` file with the following: 

a. Change the executor: replace with `executor = LocalExecutor`

b. Change SqlAlchemy connection: replace with `sql_alchemy_conn = postgresql+psycopg2://postgres_local:airflow@localhost/postgres_db`

### Run the following commands within the terminal:

Note: you will first be prompted to create a new user. Create a new user with your own credentials. Do make sure your `postgres_local` Conn Id is still present. Otherwise, recreate that Conn Id. 

a. Run `airflow db init`

b. Re-run the airflow webserver and scheduler. 



