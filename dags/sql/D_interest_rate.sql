/*
Object: Dimension table for interest rate 
Author: Group 10
Script Date: Not Dated
Description: Transfers distinct records from interest rate 
present in the staging area to data warehouse
*/

SELECT DISTINCT interest_rate.*
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.final_interest_rate` interest_rate
LEFT JOIN 
`{{ params.project_id }}.{{ params.staging_dataset }}.final_hist_prices` stocks
ON 
interest_rate.Date = stocks.Date
WHERE 
/* Ensures records in interest rate has dates in sync
with the fact table (stocks) */
interest_rate.Date <= stocks.Date