import os
import sys
from glob import glob

'''
- download data from sec.gov using "wget + url"
- unzip the files
- move unzipped files to S3 buckect "my-insight-data/logfiles+year"

require one argument such as '2016.txt'
need to configure aws credentials before running this script
'''

urls_path = sys.argv[1]
with open(urls_path, 'r') as urls:
    for url in urls:
        os.system('wget ' + url.strip())
        url_split = url.strip().split('/')
        filename = url_split[-1]
        os.system('yes | unzip ' + filename)
        os.system('rm *.zip')
        os.system('rm ' + 'README.txt')

for csv_file in glob('*.csv'):
    print('sending ' + csv_file + ' to s3')
    s3_foldername = 'logfiles' + urls_path.split('.')[0]
    s3_path = ' s3://my-insight-data/' + s3_foldername + '/'
    os.system('aws s3 mv ' + csv_file + s3_path)
