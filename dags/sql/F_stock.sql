/*
Object: Fact table for stocks
Author: Group 10
Script Date: Not Dated
Description: Combines data from commodities, exchange rate, 
interest rate, inflation rate and companies' financials
to form the fact table for end users to query from
*/

SELECT DISTINCT 
stocks.*,
exchange_rate.usd_sgd,interest_rate.sora, financials.value as networth,
exchange_rate.EXR_ID, interest_rate.INR_ID, -- Unique IDs from dimensions
financials.id AS FIN_ID, inflation.id AS INFL_ID  -- Unique IDs from dimensions
FROM 
`{{ params.project_id }}.{{ params.staging_dataset }}.final_hist_prices` stocks
LEFT JOIN 
`{{ params.project_id }}.{{ params.staging_dataset }}.distinct_exchange_rate` exchange_rate
ON 
exchange_rate.Date = stocks.Date
LEFT JOIN
`{{ params.project_id }}.{{ params.staging_dataset }}.distinct_interest_rate` interest_rate
ON
interest_rate.Date = stocks.Date
LEFT JOIN 
`{{ params.project_id }}.{{ params.staging_dataset }}.inflation_key` inflation
ON 
-- Inflation will lag behind the stocks data by 1 year
(EXTRACT(YEAR FROM inflation.YEAR)+1) = EXTRACT(YEAR FROM stocks.date)
LEFT JOIN 
`{{ params.project_id }}.{{ params.staging_dataset }}.reformat_financials_ratios` financials
ON 
-- Financials data will lag behind stocks data by 1 year
financials.ticker = stocks.Stock AND (EXTRACT(YEAR FROM financials.YEAR)+1) = EXTRACT(YEAR FROM stocks.date) AND financials.type = 'networth'


