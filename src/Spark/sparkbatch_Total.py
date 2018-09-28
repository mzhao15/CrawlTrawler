import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import psycopg2
from psycopg2 import extras
from params import pql_params

'''
this batch job is to get the number of visits by human and robot to each company
argument (date of the log file), example: 2016-01-01
csv file column names:
    'ip', 'date', 'time', 'zone', 'cik', 'accession', 'extention', 'code',
    'size','idx', 'norefer', 'noagent', 'find', 'crawler', 'browser'
'''


class CountVisits:
    def __init__(self, data_path, date):
        sc = SparkContext().getOrCreate()
        sc.setLogLevel("ERROR")
        self.spark = SparkSession(sc)
        raw = sc.textFile(data_path)
        # parse the data by comma and remove the header
        self.data = raw.map(lambda line: str(line).split(',')) \
            .filter(lambda x: x[0] != 'ip')
        self.date = date
        # extract the detected robot ip list (on master node)
        try:
            self.db_conn = psycopg2.connect(**pql_params)
        except Exception as er1:
            print('cannot connect to PostgreSQL database\n')
            print(str(er1))
        self.cur = self.db_conn.cursor(cursor_factory=extras.DictCursor)
        self.createtable()
        self.robot_ip_list = self.get_robot_ip()
        self.cur.close()
        self.db_conn.close()

    def get_robot_ip(self):
        ip_list = []
        self.cur.execute("SELECT ip FROM robot_ip WHERE detected_date=%s;", (self.date,))
        # self.cur.execute("SELECT ip FROM robot_ip;")
        records = self.cur.fetchall()  # return a list of lists
        for record in records:
            ip_list.append(record['ip'])
        return ip_list

    def createtable(self):
        '''create two tables: one for total visits, one for visits by humans only'''
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS total (id serial PRIMARY KEY, \
                                                visit_date date, \
                                                cik varchar(50), \
                                                num_of_visits int);")
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS human (id serial PRIMARY KEY, \
                                                visit_date date, \
                                                cik varchar(50), \
                                                num_of_visits int);")
        self.db_conn.commit()
        return

    def percompanyperIP(self):
        ''' get the number of visits to each company by each IP '''
        ''' input: the parsed raw data: self.data  '''
        ''' output: (ip+date+cik, count) '''
        return self.data.map(lambda line: (','.join((line[0], line[1], line[4])), 1)) \
            .reduceByKey(lambda v1, v2: v1+v2)

    def totalvisits(self, rdd):
        def inserttotal(records):
            ''' insert processed records into "total" in batch '''
            try:
                db_conn = psycopg2.connect(**pql_params)
            except Exception as er1:
                print('cannot connect to PostgreSQL database\n')
                print(str(er1))
            cur = db_conn.cursor()
            cur.execute(
                "PREPARE inserts AS INSERT INTO total (visit_date, cik, num_of_visits) \
                                                        VALUES ($1, $2, $3);")
            extras.execute_batch(cur, "EXECUTE inserts (%s, %s, %s)", records)
            cur.execute("DEALLOCATE inserts")
            db_conn.commit()
            cur.close()
            db_conn.close()

        # self.counter(self.data).foreachPartition(inserttotal)
        rdd.map(lambda count: (count[0].split(','), count[1])) \
            .map(lambda count: (','.join((count[0][1], count[0][2])), count[1])) \
            .reduceByKey(lambda v1, v2: v1+v2) \
            .map(lambda count: (count[0].split(','), count[1])) \
            .map(lambda count: (count[0][0], count[0][1], count[1])) \
            .foreachPartition(inserttotal)

    def humanvisits(self, rdd):
        ''' input rdd: count of visits per company per IP '''
        def inserthuman(records):
            try:
                db_conn = psycopg2.connect(**pql_params)
            except Exception as er1:
                print('cannot connect to PostgreSQL database\n')
                print(str(er1))
            cur = db_conn.cursor()
            cur.execute(
                "PREPARE inserts AS INSERT INTO human (visit_date, cik, num_of_visits) \
                                                        VALUES ($1, $2, $3);")
            extras.execute_batch(cur, "EXECUTE inserts (%s, %s, %s)", records)
            cur.execute("DEALLOCATE inserts")
            db_conn.commit()
            cur.close()
            db_conn.close()

        filters = self.robot_ip_list
        # count the number of visits by humans to each company
        rdd.map(lambda count: (count[0].split(','), count[1])) \
            .map(lambda count: (count[0][0], count[0][1], count[0][2], count[1])) \
            .filter(lambda line: line[0] not in filters) \
            .map(lambda line: (','.join((line[1], line[2])), line[3])) \
            .reduceByKey(lambda v1, v2: v1+v2) \
            .map(lambda count: (count[0].split(','), count[1])) \
            .map(lambda count: (count[0][0], count[0][1], count[1])) \
            .foreachPartition(inserthuman)

    def run(self):
        ''' get the total number of visits to each company
            get the total number of visits by human to each company'''
        rdd = self.percompanyperIP()
        self.totalvisits(rdd)
        self.humanvisits(rdd)
        return


if __name__ == '__main__':

    if len(sys.argv) > 2:
        print('too many arguments\n')
        exit()
    # get the folder name and filename
    year, month, day = sys.argv[1].split('-')
    foldername = 'logfiles' + year
    filename = 'log' + ''.join((year, month, day)) + '.csv'
    data_path = 's3a://my-insight-data/' + foldername + '/' + filename
    visits = CountVisits(data_path, sys.argv[1])
    visits.run()
