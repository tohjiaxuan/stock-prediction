-- SELECT cast(ticker as string) as Ticker, cast(title as string) as Title, cast(date as date) as Date, cast(link as string) as Link, 
--     cast(source as string) as Source, cast(comments as string) as Comments
-- FROM `{{ params.project_id }}.{{ params.staging_dataset }}.join_financial_news`

SELECT DISTINCT * FROM `{{ params.project_id }}.{{ params.staging_dataset }}.join_financial_news`
