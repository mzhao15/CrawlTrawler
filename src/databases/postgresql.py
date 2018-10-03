
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import datetime

params = {
    'database': 'my_insight_db',
    'user': 'mzhao15',
    'password': 'zhaomeng148097',
    'host': 'mypqlinstance.cewczr0j0xld.us-east-1.rds.amazonaws.com',
    'port': 5432
}

try:
    conn = psycopg2.connect(**params)
except Exception as er1:
    print("Unable to connect to the database")
    print(str(er1))

# Open a cursor to perform database operations
cur = conn.cursor(cursor_factory=RealDictCursor)
# Execute a command: this creates a new table
# cur.execute("CREATE TABLE IF NOT EXISTS test (id serial PRIMARY KEY, num integer, data varchar);")
# Execute a command: this inserts a new record
# cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (200, "JSON"))
# cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (500, "wrc"))
cik = '1542574.0'
start_date = '2016-01-01'
end_date = '2016-01-09'
# Execute command to query the table.
cur.execute("SELECT DISTINCT visit_date, \
                             num_of_visits \
                        FROM total WHERE cik=%s AND (visit_date BETWEEN %s AND %s) \
                    ORDER BY visit_date;", (cik, start_date, end_date))
# cur.execute("SELECT t.visit_date AS visit_date, \
#                     t.num_of_visits AS total_visits, \
#                     h.num_of_visits AS human_visits \
#             FROM total t JOIN human AS h \
#             ON t.cik=h.cik AND t.visit_date=h.visit_date \
#             WHERE t.cik=%s AND (t.visit_date BETWEEN %s AND %s) \
#             ORDER BY visit_date;", (cik, start_date, end_date))
# After executing the query, need to define a list to put the results in: rows
# rows = cur.fetchall()
# print(type(rows))
# print(rows)
# dates = []
# visits = []
# for row in rows:
#     dates.append(row['visit_date'])
#     visits.append(row['num_of_visits'])


def myconverter(realdate):
    if isinstance(realdate, datetime.date):
        return realdate.__str__()


raw = cur.fetchall()
if not raw:
    print('no data')
print(json.dumps(raw, indent=2, default=myconverter))

# Execute a command: drop a table
# cur.execute("DROP TABLE IF EXISTS test")

# Make the changes to the database persistent
conn.commit()
cur.close()
conn.close()
