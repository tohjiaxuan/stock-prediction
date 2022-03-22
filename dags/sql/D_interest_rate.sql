/* after confirming everything, change the create or replace to just create table */

CREATE OR REPLACE TABLE`{{ params.dwh_dataset }}.D_interest_rate` AS 
SELECT DISTINCT *
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.distinct_interest_rate