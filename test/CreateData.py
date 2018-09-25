import csv
import os
from datetime import datetime, timedelta

'''
create a dataset large than 5 GB for test
'''

output_filename = 'log20150101.csv'
date = datetime(2016, 1, 1).date()
with open(output_filename, 'w') as output:
    fieldnames = ['ip', 'date', 'time', 'zone', 'cik', 'accession',
                  'extention', 'code', 'size', 'idx', 'norefer', 'noagent', 'find', 'crawler', 'browser']
    csv_writer = csv.DictWriter(output, fieldnames=fieldnames)
    csv_writer.writeheader()
    for i in range(10):
        strdate = date.strftime("%Y-%m-%d")
        year, month, day = strdate.split('-')
        foldername = 'logfiles' + year
        filename = 'log' + ''.join((year, month, day)) + '.csv'
        data_path = 's3://my-insight-data/' + foldername + '/' + filename
        os.system('aws s3 cp ' + data_path + ' /home/ubuntu/')
        with open(filename, 'r') as input:
            csv_reader = csv.DictReader(input)
            for line in csv_reader:
                csv_writer.writerow(line)
        date = date + timedelta(days=1)

os.system('aws s3 mv ' + output_filename + ' s3://my-insight-data/logfiles2015/')
os.system('rm *.csv')
