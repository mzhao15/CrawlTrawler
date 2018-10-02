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


def batch_insert(table_name, records):
    ''' insert processed records into tables in batch '''
    try:
        db_conn = psycopg2.connect(**pql_params)
    except Exception as er1:
        print('cannot connect to PostgreSQL database\n')
        print(str(er1))
    cur = db_conn.cursor()
    cur.execute(
        "PREPARE stmt AS INSERT INTO {} (visit_date, cik, num_of_visits) \
                                                VALUES ($1, $2, $3);".format(table_name))
    extras.execute_batch(cur, "EXECUTE stmt (%s, %s, %s)", records)
    cur.execute("DEALLOCATE stmt")
    db_conn.commit()
    cur.close()
    db_conn.close()
    return


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
        except Exception as er2:
            print('cannot connect to PostgreSQL database\n')
            print(str(er2))
        self.cur = self.db_conn.cursor(cursor_factory=extras.DictCursor)
        self.create_table('total')
        self.create_table('human')
        self.robot_ip_list = self.get_robot_ip()
        self.cur.close()
        self.db_conn.close()

    def get_robot_ip(self):
        ''' get the list of robot ips from database'''
        ip_list = []
        self.cur.execute("SELECT ip FROM robot_ip WHERE detected_date=%s;", (self.date,))
        # self.cur.execute("SELECT ip FROM robot_ip;")
        records = self.cur.fetchall()
        for record in records:
            ip_list.append(record['ip'])
        return ip_list

    def create_table(self, table_name):
        ''' create a table named as table_name, table schema is predefined '''
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS {} (id serial PRIMARY KEY, \
                                                visit_date date, \
                                                cik varchar(50), \
                                                num_of_visits int);".format(table_name))
        self.db_conn.commit()
        return

    def count_percompany_perIP(self):
        ''' get the number of visits to each company by each IP
            input: the parsed raw data: self.data
            output: (ip+date+cik, count) '''
        return self.data.map(lambda line: (','.join((line[0], line[1], line[4])), 1)) \
            .reduceByKey(lambda v1, v2: v1+v2)

    def total_visit(self, count_percompany_perIP):
        '''     '''

        def insert(records):
            ''' insert processed records into "total" in batch '''
            batch_insert('total', records)

        count_percompany_perIP.map(lambda count: (count[0].split(','), count[1])) \
            .map(lambda count: (','.join((count[0][1], count[0][2])), count[1])) \
            .reduceByKey(lambda v1, v2: v1+v2) \
            .map(lambda count: (count[0].split(','), count[1])) \
            .map(lambda count: (count[0][0], count[0][1], count[1])) \
            .foreachPartition(insert)

    def human_visit(self, count_percompany_perIP):
        '''  '''

        def insert(records):
            batch_insert('human', records)

        filters = self.robot_ip_list
        # count the number of visits by humans to each company
        count_percompany_perIP.map(lambda count: (count[0].split(','), count[1])) \
            .map(lambda count: (count[0][0], count[0][1], count[0][2], count[1])) \
            .filter(lambda line: line[0] not in filters) \
            .map(lambda line: (','.join((line[1], line[2])), line[3])) \
            .reduceByKey(lambda v1, v2: v1+v2) \
            .map(lambda count: (count[0].split(','), count[1])) \
            .map(lambda count: (count[0][0], count[0][1], count[1])) \
            .foreachPartition(insert)

    def run(self):
        ''' get the total number of visits to each company
            get the total number of visits by human to each company'''
        count_percompany_perIP = self.count_percompany_perIP()
        self.total_visit(count_percompany_perIP)
        self.human_visit(count_percompany_perIP)
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
