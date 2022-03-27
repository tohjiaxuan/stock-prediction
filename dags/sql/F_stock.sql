SELECT DISTINCT 
stocks.*,
exchange_rate.usd_sgd,interest_rate.sora, financials.value as networth,
exchange_rate.EXR_ID, interest_rate.INR_ID,
financials.id as FIN_ID, inflation.id as INFL_ID
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
LEFT JOIN 
`{{ params.project_id }}.{{ params.staging_dataset }}.inflation_key` inflation
ON 
EXTRACT(YEAR FROM inflation.YEAR) = EXTRACT(YEAR FROM stocks.date)
LEFT JOIN 
`{{ params.project_id }}.{{ params.staging_dataset }}.reformat_financials_ratios` financials
ON 
financials.ticker = stocks.Stock and EXTRACT(YEAR FROM financials.YEAR) = EXTRACT(YEAR FROM stocks.date)
where financials.type = 'networth'


