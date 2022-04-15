/*
Object: Dimension table for yearly inflation rate 
Author: Group 10
Script Date: Not Dated
Description: Transfers distinct records from inflation rate 
present in the staging area to data warehouse
*/

SELECT DISTINCT *
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.inflation_key`