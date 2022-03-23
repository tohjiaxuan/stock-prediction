/* after confirming everything, change the create or replace to just create table */
CREATE OR REPLACE TABLE`{{ params.project_id }}.{{ params.dwh_dataset }}.D_financials` AS 
SELECT DISTINCT *
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.reformat_financials_ratios`