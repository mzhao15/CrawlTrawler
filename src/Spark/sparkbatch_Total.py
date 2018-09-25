import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import psycopg2
from psycopg2 import extras
from params import pql_params

'''
this batch job is to get the number of visits by human and robot to each company
argument (date of the log file), example: 2016-01-01
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
        records = self.cur.fetchall()  # return a list of lists
        for record in records:
            ip_list.append(record['ip'])
        return ip_list

    def createtable(self):
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS human (id serial PRIMARY KEY, \
                                                visit_date date, \
                                                cik varchar(50), \
                                                num_of_visits int);")
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS robot (id serial PRIMARY KEY, \
                                                visit_date date, \
                                                cik varchar(50), \
                                                num_of_visits int);")
        self.db_conn.commit()
        return

    def counter(self, rdd):
        '''count the number of visits to each company (CIK)'''
        '''line[1]: date, line[4]:CIK '''
        return rdd.map(lambda line: (line[1], line[4])) \
            .map(lambda x: (','.join(x), 1)) \
            .reduceByKey(lambda v1, v2: v1+v2) \
            .map(lambda count: (count[0].split(','), count[1])) \
            .map(lambda count: (count[0][0], count[0][1], count[1]))

    def humanvisits(self):
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
        self.counter(self.data.filter(
            lambda line: line[0] not in filters)).foreachPartition(inserthuman)

    def robotvisits(self):
        def insertrobot(records):
            try:
                db_conn = psycopg2.connect(**pql_params)
            except Exception as er1:
                print('cannot connect to PostgreSQL database\n')
                print(str(er1))
            cur = db_conn.cursor()
            cur.execute(
                "PREPARE inserts AS INSERT INTO robot (visit_date, cik, num_of_visits) \
                                                        VALUES ($1, $2, $3);")
            extras.execute_batch(cur, "EXECUTE inserts (%s, %s, %s)", records)
            cur.execute("DEALLOCATE inserts")
            db_conn.commit()
            cur.close()
            db_conn.close()

        filters = self.robot_ip_list
        self.counter(self.data.filter(
            lambda line: line[0] in filters)).foreachPartition(insertrobot)
    # def robotvisits(self):
    #     filters = self.robot_ip_list
    #     df = self.data.toDF(['ip', 'date', 'time', 'zone', 'cik', 'accession',
    #                          'extention', 'code', 'size', 'idx', 'norefer', 'noagent',
    #                          'find', 'crawler', 'browser'])
    #     drop_list = ['time', 'zone', 'accession', 'extention', 'code', 'size', 'idx',
    #                  'norefer', 'noagent', 'find', 'crawler', 'browser']
    #     df.drop(*drop_list).filter(df.ip.isin(filters)).groupBy('date', 'cik').count() \
    #         .withColumnRenamed('date', 'visit_date').withColumnRenamed('count', 'num_of_visits') \
    #         .write \
    #         .format("jdbc") \
    #         .option("url", "jdbc:postgresql://mypqlinstance.cewczr0j0xld.us-east-1.rds.amazonaws.com:5432/my_insight_db") \
    #         .option("dbtable", 'robot') \
    #         .option("driver", "org.postgresql.Driver") \
    #         .option("user", "mzhao15") \
    #         .option("password", "zhaomeng148097") \
    #         .mode("append") \
    #         .save()

    def run(self):
        ''' get the number of visits by human and robot to each company'''

        # filters = self.robot_ip_list
        # counts = self.counter(self.data.filter(lambda line: line[0] not in filters)).collect()
        # extras.execute_batch(self.cur, "INSERT INTO human(visit_date, cik, num_of_visits) \
        #                      VALUES( %s, %s, %s)", counts)
        # counts = self.counter(self.data.filter(lambda line: line[0] in filters)).collect()
        # extras.execute_batch(self.cur, "INSERT INTO robot(visit_date, cik, num_of_visits) \
        #                      VALUES( %s, %s, %s)", counts)
        # self.db_conn.commit()

        self.humanvisits()
        self.robotvisits()

        return


if __name__ == '__main__':
    # get the folder name and filename
    if len(sys.argv) > 2:
        print('too many arguments\n')
        exit()
    year, month, day = sys.argv[1].split('-')
    foldername = 'logfiles' + year
    filename = 'log' + ''.join((year, month, day)) + '.csv'
    # data_path = "s3a://my-insight-data/logfiles2016/log20160101.csv"
    data_path = 's3a://my-insight-data/' + foldername + '/' + filename
    visits = CountVisits(data_path, sys.argv[1])
    visits.run()
