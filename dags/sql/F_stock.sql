SELECT DISTINCT 
stocks.*,
exchange_rate.usd_sgd,interest_rate.sora,
exchange_rate.EXR_ID, interest_rate.INR_ID
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
