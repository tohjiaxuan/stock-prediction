# Creation of DATA WAREHOUSE on BigQuery for Stock Analysis (Prediction) & Sentiment Analysis

# Changes to Airflow Configuration
1. Head to `airflow.cfg` and change `enable_xcom_pickling = True`


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

Note: you will first be prompted to create a new user. Create a new user with your own credentials. 

a. Run `airflow db init`

b. Re-run the airflow webserver and scheduler. 



