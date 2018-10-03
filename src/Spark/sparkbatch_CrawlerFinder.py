import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import psycopg2
from psycopg2 import extras
from datetime import datetime, timedelta
from params import pql_params

'''
this batch job is to identify the web crawler robot IPs
argument (date of the log file), example: 2016-01-01
'''


class CrawlerIPFinder:
    def __init__(self, data_path, date):
        sc = SparkContext().getOrCreate()
        sc.setLogLevel("ERROR")
        self.spark = SparkSession(sc)
        raw = sc.textFile(data_path)
        # parse the data by comma and remove the header
        self.data = raw.map(lambda line: str(line).split(',')) \
            .filter(lambda x: x[0] != 'ip')
        self.date = date
        # connection on the master node to create table and remove old records
        try:
            self.db_conn = psycopg2.connect(**pql_params)
        except Exception as er1:
            print('cannot connect to PostgreSQL database\n')
            print(str(er1))
        self.cur = self.db_conn.cursor(cursor_factory=extras.DictCursor)
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS robot_ip (id serial PRIMARY KEY, \
                                       detected_date date, \
                                                  ip varchar(20));")
        self.db_conn.commit()

    def closedb(self):
        '''close the database connnection on the master node'''
        self.cur.close()
        self.db_conn.close()

    def deleteIPs(self):
        '''
        remove robot IPs which were detected more than 30 days ago
        remove duplicated IPs
        '''
        tdy = datetime.strptime(self.date, '%Y-%m-%d')
        checkday = tdy - timedelta(days=30)
        # delete IPs which were detected more than 30 days ago
        self.cur.execute('DELETE FROM robot_ip \
                                WHERE detected_date<%s', (checkday,))
        # delete duplicate IPs
        self.cur.execute('DELETE FROM robot_ip a \
                                USING robot_ip b \
                                WHERE a.detected_date < b.detected_date AND a.ip=b.ip;')
        self.db_conn.commit()
        self.closedb()

    def percompanypermin(self):
        '''
        coun the number of visits to each company per minute by each IP
        return ('ip,datetime,cik', count)    Note: datetime='year-month-day-hour-minute'
        '''
        return self.data.map(lambda line: (line[0], line[1], line[2].split(':'), line[4])) \
            .map(lambda x: (x[0], (x[1], x[2][0], x[2][1]), x[3])) \
            .map(lambda y: (y[0], '-'.join(y[1]), y[2])) \
            .map(lambda z: (','.join(z), 1)) \
            .reduceByKey(lambda v1, v2: v1+v2)

    def permin(self, rdd):
        '''
        input from self.percompanypermin()
        count the number of visits per minute by each IP
        return ('ip+datetime', count)   Note: datetime='year-month-day-hour-minute'
        '''
        return rdd.map(lambda line: (line[0].split(','), line[1])) \
            .map(lambda x: (','.join((x[0][0], x[0][1])), x[1])) \
            .reduceByKey(lambda v1, v2: v1+v2)

    def get_totalcompanypermin(self, rdd):
        '''
        input from self.percompanypermin()
        find IPs which visit more than 3 companies per minute
        '''
        return rdd.filter(lambda count: count[1] > 3) \
            .map(lambda count: count[0].split(',')) \
            .map(lambda x: (x[1][:10], x[0]))

    def get_totalvisitpermin(sefl, rdd):
        '''
        input from self.permin()
        find IPs which visits more than 25 items per minute
        '''
        return rdd.filter(lambda count: count[1] > 25) \
            .map(lambda count: count[0].split(',')) \
            .map(lambda x: (x[1][:10], x[0]))

    def get_totalvisitperday(self, rdd):
        '''
        input from self.permin()
        find IPs which visit more than 500 times in a single day
        '''
        return rdd.map(lambda line: (line[0].split(','), line[1])) \
            .map(lambda x: (','.join((x[0][0], x[0][1][:10])), x[1]))\
            .reduceByKey(lambda v1, v2: v1+v2) \
            .filter(lambda count: count[1] > 500) \
            .map(lambda count: (count[0].split(',')[1], count[0].split(',')[0]))

    def run(self):
        '''
        save all the detected robot IPs to the database with the detected date
        '''
        def insert(records):
            try:
                db_conn = psycopg2.connect(**pql_params)
            except Exception as er2:
                print('cannot connect to PostgreSQL database\n')
                print(str(er2))
            cur = db_conn.cursor()
            cur.execute(
                "PREPARE inserts AS INSERT INTO robot_ip (detected_date, ip) \
                                                  VALUES ($1, $2);")
            extras.execute_batch(cur, "EXECUTE inserts (%s, %s)", records)
            cur.execute("DEALLOCATE inserts")
            db_conn.commit()
            cur.close()
            db_conn.close()

        # clear old robot IPs and duplicated robot IPs
        self.deleteIPs()
        cachedrdd = self.percompanypermin()
        newcachedrdd = self.permin(cachedrdd)
        self.get_totalcompanypermin(cachedrdd).union(self.get_totalvisitpermin(newcachedrdd)) \
            .union(self.get_totalvisitperday(newcachedrdd)).distinct().foreachPartition(insert)


if __name__ == "__main__":
    # get the folder name and filename
    if len(sys.argv) > 2:
        print('too many arguments\n')
        exit()
    year, month, day = sys.argv[1].split('-')
    foldername = 'logfiles' + year
    filename = 'log' + ''.join((year, month, day)) + '.csv'
    data_path = 's3a://my-insight-data/' + foldername + '/' + filename
    finder = CrawlerIPFinder(data_path, sys.argv[1])
    finder.run()
