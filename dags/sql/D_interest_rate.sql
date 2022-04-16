SELECT DISTINCT interest_rate.*
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.casted_interest_rate` interest_rate
LEFT JOIN 
`{{ params.project_id }}.{{ params.staging_dataset }}.final_hist_prices` stocks
ON 
interest_rate.Date = stocks.Date
WHERE 
interest_rate.Date <= stocks.Date