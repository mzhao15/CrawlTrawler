import os
import glob

urls_path = '/home/meng/Documents/Insight-Meng/DataPreprocessing/URL_list.html'

with open(urls_path, 'r') as urls_file:
    urls = urls_file.read()
    urls = urls.split()
    for i, url in enumerate(urls):
        print(i)
        if i > 6 and i < 8:
            print(url)
            # os.system('wget ' + url)
            # os.system('yes yes | unzip "*.zip"')
        elif i >= 8:
            break
os.chdir('/home/meng/Documents/Insight-Meng/DataPreprocessing/')
print(os.getcwd())
for file in glob.glob("*.py"):
    print(file)
    # os.sytem('aws s3 cp file s3://*****/data/')
