import os
from glob import glob

urls_path = '2016.txt'
with open(urls_path, 'r') as urls:
    i = 0
    for url in urls:
        if i < 3:
            os.system('wget ' + url.strip())
            url_split = url.split()
            filename = url_split[-1]
            os.system('yes | unzip' + filename)
            i += 1
        else:
            break
# for logfile in glob("*.csv"):
#     print(logfile)
#     os.sytem('aws s3 mv logfile s3://my-insight-data/logfiles2016/')
