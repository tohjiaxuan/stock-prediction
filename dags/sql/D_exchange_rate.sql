SELECT DISTINCT exchange_rate.*
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.distinct_exchange_rate` exchange_rate
LEFT JOIN 
`{{ params.project_id }}.{{ params.staging_dataset }}.final_hist_prices` stocks
ON 
exchange_rate.Date = stocks.Date
WHERE 
exchange_rate.Date <= stocks.Date