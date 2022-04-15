# Creation of ETL pipeline using Apache Airlow and Data Warehouse on BigQuery for Stock Analysis (Prediction) & Sentiment Analysis

# General Configurations needed
1. Head to `airflow.cfg` and change `enable_xcom_pickling = True`
2. Create GCS bucket on Google Cloud Platform
a. Set location to asia-southeast1 (Singapore)
b. Turn off object versioning, do not choose a retention policy and set encryption type to be google-managed key
c. Set public access to be not public
d. Developer should set the permissions by giving corresponding users the right access and role

3. In the Airflow webserver, head to `Admin` > `Connections`. 

a. Edit the `google_cloud_default` Conn Id by pasting the Google ServiceKey (in JSON format) in the `Keyfile JSON` field. 

b. Set up PostgreSQL if you have not done so and then create a `postgres_local` Conn Id with your credentials - set `Host` and `Port` as `localhost` and `5432` respectively. 

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



