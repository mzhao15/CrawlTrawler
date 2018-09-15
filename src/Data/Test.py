import os
from glob import glob

urls_path = '2016.txt'
with open(urls_path, 'r') as urls:
    for i, url in enumerate(urls):
        if i > 59:
            os.system('wget ' + url.strip())
            url_split = url.strip().split('/')
            filename = url_split[-1]
            os.system('yes | unzip ' + filename)
            os.system('rm *.zip')
            os.system('rm ' + 'README.txt')

for csv_file in glob('*.csv'):
    print('sending ' + csv_file + ' to s3')
    os.system('aws s3 mv ' + csv_file + ' s3://my-insight-data/logfiles2016/')
