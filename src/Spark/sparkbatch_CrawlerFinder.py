import sys
from pyspark import SparkContext
import psycopg2
from psycopg2 import extras
from datetime import datetime, time, timedelta
from params import pql_params

'''
this batch job is to identify the web crawler robot IPs
argument (date of the log file), example: 2016-01-01
'''


class CrawlerIPFinder:
    def __init__(self, data_path):
        sc = SparkContext().getOrCreate()
        sc.setLogLevel("ERROR")
        raw = sc.textFile(data_path)
        # parse the data by comma and remove the header
        self.data = raw.map(lambda line: str(line).split(',')) \
            .filter(lambda x: x[0] != 'ip')
        self.date = self.data.first()[1]
        try:
            self.db_conn = psycopg2.connect(**pql_params)
        except Exception as er1:
            print('cannot connect to PostgreSQL database\n')
            print(str(er1))
        self.cur = self.db_conn.cursor(cursor_factory=extras.DictCursor)
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS table_robot_ip (id serial PRIMARY KEY, \
                                                        ip varchar(80), \
                                                        detected_num int, \
                                                        detected_date date, \
                                                        flag int);")

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

    def insertIPs(self, records, flag):
        ''' insert the record if not exists'''
        new_records = []
        for record in records:
            new_records.append((record[0], record[1], self.date, flag))

        self.cur.execute(
            "PREPARE insertIPs AS INSERT INTO table_robot_ip (ip, detected_num, detected_date, flag) \
            VALUES ($1, $2, $3, $4);")
        extras.execute_batch(self.cur, "EXECUTE insertIPs (%s, %s, %s, %s)", new_records)
        self.cur.execute("DEALLOCATE insertIPs")
        self.db_conn.commit()

    def deleteIPs(self):
        '''remove robot IPs which have not been active for more than 30 days'''
        tdy = datetime.strptime(self.date, '%Y-%m-%d')
        checkday = tdy - timedelta(days=30)
        self.cur.execute("SELECT id, ip FROM table_robot_ip WHERE detected_date=%s;", (checkday,))
        records = self.cur.fetchall()
        if not records:
            '''the first 30 days'''
            return
        self.cur.execute("SELECT ip FROM table_robot_ip WHERE detected_date>%s;", (checkday,))
        new_records = self.cur.fetchall()
        ips = []  # list of ips deteted after the check day
        for record in new_records:
            if record['ip'] not in ips:
                ips.append(record['ip'])
        delete_list = []  # list of ips need to be deleted
        for i in range(len(records)):
            if records[i]['ip'] not in ips:
                delete_list.append(records[i]['id'])
        self.cur.execute('DELETE FROM table_robot_ip WHERE id IN %s', (tuple(delete_list),))
        self.db_conn.commit()

    def get_totaldownloadpermin2(self):
        '''sliding window to find the robot IPs'''
        def insert(records):
            tempdb_conn = psycopg2.connect(**pql_params)
            tempcur = tempdb_conn.cursor()
            tempcur.execute(
                "PREPARE stmt AS INSERT INTO table_robot_ip (ip, detected_num) VALUES ($1, $2);")
            extras.execute_batch(tempcur, "EXECUTE stmt (%s, %s)", records)
            tempcur.execute("DEALLOCATE stmt")
            tempdb_conn.commit()
            tempcur.close()
            tempdb_conn.close()
        ip_list = []
        windowstart = datetime.combine(datetime.strptime(self.date, '%Y-%m-%d'), time(0, 0, 0))
        # tomorrow = windowstart + timedelta(days=1)
        tomorrow = datetime(2016, 1, 1, 1, 0, 0)
        # 'ip', '2016-01-01 00:00:00'
        parsed_data = self.data.map(lambda x: (x[0], datetime.strptime(
            ' '.join((x[1], x[2])), '%Y-%m-%d %H:%M:%S')))
        windowend = windowstart + timedelta(seconds=10)
        while windowend <= tomorrow:
            records = parsed_data.filter(lambda x: x[1] >= windowstart and x[1]
                                         <= windowend).map(lambda x: (x[0], 1)) \
                .reduceByKey(lambda v1, v2: v1+v2) \
                .filter(lambda count: count[1] > 25).collect()
            ip_list.extend(records)
            windowstart = windowstart + timedelta(seconds=1)
            windowend = windowstart + timedelta(seconds=10)
        return

    def run(self):
        '''
        save all the detected robot IPs to datases with flags
        need to update the database based on the retention time
        '''
        # self.get_totaldownloadpermin2()
        self.deleteIPs()  # clear not active robot IPs
        records = self.get_totalcompanypermin().collect()
        self.insertIPs(records, flag=0)
        records = self.get_totaldownloadpermin().collect()
        self.insertIPs(records, flag=1)
        records = self.get_totaldownloadperday().collect()
        self.insertIPs(records, flag=2)

        self.cur.close()
        self.db_conn.close()
        return


if __name__ == "__main__":
    # get the folder name and filename
    if len(sys.argv) > 2:
        print('too many arguments\n')
    year, month, day = sys.argv[1].split('-')
    foldername = 'logfiles' + year
    filename = 'log' + ''.join((year, month, day)) + '.csv'
    # data_path = "s3a://my-insight-data/logfiles2016/log20160101.csv"
    data_path = 's3a://my-insight-data/' + foldername + '/' + filename
    finder = CrawlerIPFinder(data_path)
    finder.run()
