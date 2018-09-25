import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import psycopg2
from psycopg2 import extras
from datetime import datetime, timedelta, time
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
            .map(lambda count: count[0].split(',')) \
            .map(lambda x: (x[1][:10], x[0]))

    def get_totaldownloadperday(self):
        '''
        find IPs which download more than 500 times in a single day
        '''
        return self.data.map(lambda line: (','.join((line[0], line[1])), 1)) \
            .reduceByKey(lambda v1, v2: v1+v2) \
            .filter(lambda count: count[1] > 500) \
            .map(lambda count: (count[0].split(',')[1], count[0].split(',')[0]))

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
            .map(lambda count: count[0].split(',')) \
            .map(lambda x: (x[1][:10], x[0])) \


    def closedb(self):
        self.cur.close()
        self.db_conn.close()

    def deleteIPs(self):
        '''remove robot IPs which have not been active for more than 30 days'''
        tdy = datetime.strptime(self.date, '%Y-%m-%d')
        checkday = tdy - timedelta(days=30)
        self.cur.execute("SELECT id, ip FROM robot_ip WHERE detected_date=%s;", (checkday,))
        records = self.cur.fetchall()
        if not records:
            '''the first 30 days'''
            return
        self.cur.execute("SELECT ip FROM robot_ip WHERE detected_date>%s;", (checkday,))
        new_records = self.cur.fetchall()
        ips = []  # list of ips deteted after the check day
        for record in new_records:
            if record['ip'] not in ips:
                ips.append(record['ip'])
        delete_list = []  # list of ips need to be deleted
        for i in range(len(records)):
            if records[i]['ip'] not in ips:
                delete_list.append(records[i]['id'])
        self.cur.execute('DELETE FROM robot_ip WHERE id IN %s', (tuple(delete_list),))
        # delete IPs which were detected more than 30 days ago
        self.cur.execute('DELETE FROM robot_ip WHERE detected_date<%s', (checkday,))
        self.db_conn.commit()
        self.closedb()

    # def get_totaldownloadpermin2(self):
    #     '''sliding window to find the robot IPs'''
    #     def insert(records):
    #         tempdb_conn = psycopg2.connect(**pql_params)
    #         tempcur = tempdb_conn.cursor()
    #         tempcur.execute(
    #             "PREPARE stmt AS INSERT INTO robot_ip (detected_date, ip) VALUES ($1, $2);")
    #         extras.execute_batch(tempcur, "EXECUTE stmt (%s, %s)", records)
    #         tempcur.execute("DEALLOCATE stmt")
    #         tempdb_conn.commit()
    #         tempcur.close()
    #         tempdb_conn.close()
    #     ip_list = []
    #     windowstart = datetime.combine(datetime.strptime(self.date, '%Y-%m-%d'), time(0, 0, 0))
    #     # tomorrow = windowstart + timedelta(days=1)
    #     tomorrow = datetime(2016, 1, 1, 1, 0, 0)
    #     # 'ip', '2016-01-01 00:00:00'
    #     parsed_data = self.data.map(lambda x: (x[0], datetime.strptime(
    #         ' '.join((x[1], x[2])), '%Y-%m-%d %H:%M:%S')))
    #     windowend = windowstart + timedelta(seconds=10)
    #     while windowend <= tomorrow:
    #         records = parsed_data.filter(lambda x: x[1] >= windowstart and x[1]
    #                                      <= windowend).map(lambda x: (x[0], 1)) \
    #             .reduceByKey(lambda v1, v2: v1+v2) \
    #             .filter(lambda count: count[1] > 25).collect()
    #         ip_list.extend(records)
    #         windowstart = windowstart + timedelta(seconds=1)
    #         windowend = windowstart + timedelta(seconds=10)
    #     return

    def run(self):
        '''
        save all the detected robot IPs to datases with flags
        need to update the database based on the retention time
        '''
        def insert(records):
            try:
                db_conn = psycopg2.connect(**pql_params)
            except Exception as er1:
                print('cannot connect to PostgreSQL database\n')
                print(str(er1))
            cur = db_conn.cursor()
            cur.execute(
                "PREPARE inserts AS INSERT INTO robot_ip (detected_date, ip) \
                                                        VALUES ($1, $2);")
            extras.execute_batch(cur, "EXECUTE inserts (%s, %s)", records)
            cur.execute("DEALLOCATE inserts")
            db_conn.commit()
            cur.close()
            db_conn.close()

        self.deleteIPs()  # clear not active robot IPs
        self.get_totalcompanypermin().union(self.get_totaldownloadpermin()) \
            .union(self.get_totaldownloadperday()).distinct().foreachPartition(insert)


if __name__ == "__main__":
    # get the folder name and filename
    if len(sys.argv) > 2:
        print('too many arguments\n')
        exit()
    year, month, day = sys.argv[1].split('-')
    foldername = 'logfiles' + year
    filename = 'log' + ''.join((year, month, day)) + '.csv'
    # data_path = "s3a://my-insight-data/logfiles2016/log20160101.csv"
    data_path = 's3a://my-insight-data/' + foldername + '/' + filename
    finder = CrawlerIPFinder(data_path, sys.argv[1])
    finder.run()
