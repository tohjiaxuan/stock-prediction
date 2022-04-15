/*
Object: Fact table for daily financial news
Author: Group 10
Script Date: Not Dated
Description: Transfers distinct records from financial news 
present in the staging area to data warehouse
*/

SELECT DISTINCT * 
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.join_financial_news`
