# Insight-Meng

This data engineering project built a data pipeline to batch process the EDGAR (Electronic Data Gathering, Analysis and Retrieval system) log files to monitor the most downloaded financial documents from companies.

Background
U.S. Securities and Exchange Commission (www.sec.gov) requires all companies to file registration statements and periodic reports which are available to public. The Division of Economic and Risk Analysis (DERA) has assembled information on internet search traffic for EDGAR filings through SEC.gov generally covering the period February 14, 2003 through June 30, 2017. By analyzing this datset, we can suggest the average users the 'hot' companies based on the number of visits to their filings in SEC.

Data Engineering Challenge
Data is not always clean as expected. There are noise and also fake (or even wrong) data in the raw datasets. In this project, we identify the web crawler IPs and filter these IPs, then perform batch processing to find the top visited companies everyday.

Data Pipeline
The pipeline includes two-layer of batch processing. In the first layer, we identify the web crawler IPs and save/update them in Postgres. In the second batch job, we find the top visited companies and output to our database.

