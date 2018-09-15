import os
from glob import glob

urls_path = '2016.txt'
with open(urls_path, 'r') as urls:
    i = 0
    for url in urls:
        if i < 4:
            os.system('wget ' + url.strip())
            i += 1
        else:
            break
os.system('yes "yes"| unzip "*.zip"')
# for logfile in glob("*.csv"):
#     print(logfile)
#     os.sytem('aws s3 mv logfile s3://my-insight-data/logfiles2016/')
