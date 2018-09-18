from pyspark import SparkContext
import psycopg2
from psycopg2 import extras
from params import pql_params

'''
this batch job is to get the number of visits by human and robot to each company
'''


class CountVisits:
    def __init__(self, data_path):
        raw = SparkContext().getOrCreate().textFile(data_path)
        # parse the data by comma and remove the header
        self.data = raw.map(lambda line: str(line).split(',')) \
            .filter(lambda x: x[0] != 'ip')
        self.robot_ip_list = []
        try:
            self.db_conn = psycopg2.connect(**pql_params)
        except Exception as er1:
            print('cannot connect to PostgreSQL database\n')
            print(str(er1))
        self.cur = self.db_conn.cursor(cursor_factory=extras.DictCursor)
        self.cur.execute("SELECT DISTINCT ip FROM table_robot_ip;")
        records = self.cur.fetchall()  # return a list of lists
        for record in records:
            self.robot_ip_list.append(record['ip'])
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS human_visits (id serial PRIMARY KEY, \
                                                        visit_date date, \
                                                        cik varchar(80), \
                                                        num_of_visits int);")
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS robot_visits (id serial PRIMARY KEY, \
                                                        visit_date date, \
                                                        cik varchar(80), \
                                                        num_of_visits int);")

    def counter(self, rdd):
        '''count the number of visits by IPs'''
        '''line[1]: date, line[4]:CIK '''
        return rdd.map(lambda line: (line[1], line[4])) \
            .map(lambda x: (','.join(x), 1)) \
            .reduceByKey(lambda v1, v2: v1+v2) \
            .map(lambda count: (count[0].split(','), count[1])) \
            .map(lambda count: (count[0][0], count[0][1], count[1]))
        # return visit_date, cik, num_of_visits

    def saveVisits(self):
        ''' get the number of visits by human and robot to each company'''
        filters = self.robot_ip_list
        counts = self.counter(self.data.filter(lambda line: line[0] not in filters)).collect()
        extras.execute_batch(self.cur, "INSERT INTO human_visits(visit_date, cik, num_of_visits) \
                             VALUES( %s, %s, %s)", counts)
        counts = self.counter(self.data.filter(lambda line: line[0] in filters)).collect()
        extras.execute_batch(self.cur, "INSERT INTO robot_visits(visit_date, cik, num_of_visits) \
                             VALUES( %s, %s, %s)", counts)
        self.db_conn.commit()
        self.cur.close()
        self.db_conn.close()


if __name__ == '__main__':
    data_path = "s3a://my-insight-data/logfiles2016/log20160101.csv"
    visits = CountVisits(data_path)
    visits.saveVisits()
