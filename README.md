# Creation of DATA WAREHOUSE on BigQuery for Stock Analysis (Prediction) & Sentiment Analysis

# Changes to Airflow Configuration
1. Head to `airflow.cfg` and `change enable_xcom_pickling = True`


# Steps to configure the sending of emails on task failure
1. Generate Google App password: 

a. Visit the App Passwords page (https://security.google.com/settings/security/apppasswords)

b. Select App as `airflow` and select `Generate`. 

c. Copy the generated 16 digit password. 

2. Edit `airflow.cfg` file with the following:

smtp_host = smtp.googlemail.com

smtp_user = YOUR_EMAIL_ADDRESS

smtp_password = 16_DIGIT_APP_PASSWORD

smtp_mail_from = YOUR_EMAIL_ADDRESS

