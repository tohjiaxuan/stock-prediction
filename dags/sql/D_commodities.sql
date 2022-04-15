/*
Object: Dimension table for commodities 
Author: Group 10
Script Date: Not Dated
Description: Transfers distinct records from commodities
present in the staging area to data warehouse
*/

SELECT DISTINCT *
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.final_commodity_prices`