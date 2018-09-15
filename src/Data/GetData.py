import os
from glob import glob

urls_path = '/home/meng/Documents/Insight-Meng/src/Data/2016.txt'
with open(urls_path, 'r') as urls:
    for url in urls:
        os.system('wget ' + url.strip())
        os.system('yes yes | unzip "*.zip"')
for logfile in glob("*.csv"):
    print(logfile)
    os.sytem('aws s3 mv logfile s3://my-insight-data/logfiles2016/')
