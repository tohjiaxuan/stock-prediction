/* after confirming everything, change the create or replace to just create table */

CREATE OR REPLACE TABLE`{{ params.dwh_dataset }}.F_STOCKS` AS 
SELECT DISTINCT exchange_rate.* EXCEPT(`Date`), interest_rate.* EXCEPT(`Date`), stocks.*
FROM 
`{{ params.project_id }}.{{ params.staging_dataset }}.final_hist_prices` stocks
LEFT JOIN 
`{{ params.project_id }}.{{ params.staging_dataset }}.distinct_exchange_rate` exchange_rate
ON 
exchange_rate.Date =  stocks.Date
LEFT JOIN
`{{ params.project_id }}.{{ params.staging_dataset }}.distinct_interest_rate` interest_rate
ON
interest_rate.Date = stocks.Date