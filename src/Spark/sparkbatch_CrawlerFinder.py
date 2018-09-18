from pyspark import SparkContext
import psycopg2
from params import pql_params
# from psycopg2 import extras

'''
this batch job is to identify the web crawler robot IPs
'''


class CrawlerIPFinder:
    def __init__(self, data_path):
        raw = SparkContext().getOrCreate().textFile(data_path)
        # parse the data by comma and remove the header
        self.data = raw.map(lambda line: str(line).split(',')) \
            .filter(lambda x: x[0] != 'ip')
        self.date = self.data.first()[1]
        try:
            self.db_conn = psycopg2.connect(**pql_params)
        except Exception as er1:
            print('cannot connect to PostgreSQL database\n')
            print(str(er1))

        self.cur = self.db_conn.cursor()
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS table_robot_ip (ip varchar(80), \
                                                        detected_num int, \
                                                        detected_date date, \
                                                        flag int, \
                                                        PRIMARY KEY (ip, detected_date, flag));")

    def get_totaldownloadpermin(self):
        '''
        find IPs which download more than 25 items in one minute
        '''
        return self.data.map(lambda line: (line[0], line[1], line[2].split(':'))) \
            .map(lambda x: (x[0], (x[1], x[2][0], x[2][1]))) \
            .map(lambda y: (y[0], '-'.join(y[1]))) \
            .map(lambda z: (','.join(z), 1)) \
            .reduceByKey(lambda v1, v2: v1+v2) \
            .filter(lambda count: count[1] > 25) \
            .map(lambda count: (count[0].split(',')[0], count[1]))

    def get_totalcompanypermin(self):
        '''
        find IPs which download items from more than 3 companies in one minute
        '''
        return self.data.map(lambda line: (line[0], line[1], line[2].split(':'), line[4])) \
            .map(lambda x: (x[0], (x[1], x[2][0], x[2][1]), x[3])) \
            .map(lambda y: (y[0], '-'.join(y[1]), y[2])) \
            .map(lambda z: (','.join(z), 1)) \
            .reduceByKey(lambda v1, v2: v1+v2) \
            .filter(lambda count: count[1] > 3) \
            .map(lambda count: (count[0].split(',')[0], count[1]))

    def get_totaldownloadperday(self):
        '''
        find IPs which download more than 500 times in a single day
        '''
        return self.data.map(lambda line: (line[0], 1)) \
            .reduceByKey(lambda v1, v2: v1+v2) \
            .filter(lambda count: count[1] > 500)

    def insert(self, records, flag):
        ''' insert the record if not exists'''
        for record in records:
            # self.cur.execute("SELECT ip FROM table_robot_ip WHERE ip=%s", (record[0],))
            # if not self.cur.fetchall():
            self.cur.execute("INSERT INTO table_robot_ip VALUES(%s, %s, %s, %s) \
                             ON CONFLICT(ip, detected_date, flag) DO NOTHING;",
                             (record[0], record[1], self.date, flag))
        return

    def saveIPs(self):
        '''
        save all the detected robot IPs to datases with flags
        need to update the database based on the retention time
        '''
        records = self.get_totalcompanypermin().collect()
        self.insert(records, flag=0)
        records = self.get_totaldownloadpermin().collect()  # foreachPartition(func_inserts)
        self.insert(records, flag=1)
        records = self.get_totaldownloadperday().collect()
        self.insert(records, flag=2)

        # cur.execute("DEALLOCATE inserts")
        self.db_conn.commit()
        self.cur.close()
        self.db_conn.close()
        return


if __name__ == "__main__":

    data_path = "s3a://my-insight-data/logfiles2016/log20160101.csv"
    finder = CrawlerIPFinder(data_path)
    finder.saveIPs()
