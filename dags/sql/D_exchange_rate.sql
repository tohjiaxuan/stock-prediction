/*
Object: Dimension table for exchnage rate 
Author: Group 10
Script Date: Not Dated
Description: Transfers distinct records from exchange rate 
present in the staging area to data warehouse
*/

SELECT DISTINCT exchange_rate.*
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.distinct_exchange_rate` exchange_rate
LEFT JOIN 
`{{ params.project_id }}.{{ params.staging_dataset }}.final_hist_prices` stocks
ON 
exchange_rate.Date = stocks.Date
WHERE 
/* Ensures records in exchange rate has dates in sync
with the fact table (stocks) */
exchange_rate.Date <= stocks.Date 