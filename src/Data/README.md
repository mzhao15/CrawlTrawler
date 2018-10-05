### Files

_'URL_list.html'_ includes all urls for downloading data from [sec.gov](https://www.sec.gov/dera/data) (from 2013-2017). 

_'\*.txt'_ files include the urls for downloading data from [sec.gov](https://www.sec.gov/dera/data) based on the year.

_'GetData.py'_ downloads data from sec.gov and saves to AWS S3 bucket.


### Data Format

Data are saved on a daily basis (~1-4GB, ~15 million of records) as CSV files. Each csv file has 15 columns:
- ip
- date
- time
- zone
- cik
- accession
- extention
- code
- size
- idx
- norefer
- noagent
- find
- crawler
- browser

More details about each column can be found in [docs/EDGAR_variables.pdf](docs/EDGAR_variables.pdf).
