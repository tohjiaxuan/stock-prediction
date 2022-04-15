/*
Object: Dimension table for companies' yearly financials
Author: Group 10
Script Date: Not Dated
Description: Transfers distinct records from financials
present in the staging area to data warehouse
*/

SELECT DISTINCT *
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.reformat_financials_ratios`