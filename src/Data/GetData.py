import os

urls_path = '2016.txt'
with open(urls_path, 'r') as urls:
    for url in urls:
        os.system('wget ' + url.strip())
        url_split = url.strip().split('/')
        filename = url_split[-1]
        os.system('yes | unzip ' + filename)
        csv_file = filename.replace('zip', 'csv')
        os.system('aws s3 mv ' + csv_file + ' s3://my-insight-data/logfiles2016/')
        print('sending ' + csv_file + ' to s3')
        os.system('rm log*')
        os.system('rm ' + 'README.txt')
