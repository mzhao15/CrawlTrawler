
import psycopg2
from psycopg2 import extras

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
    print("I am unable to connect to the database")
    print(str(er1))

# Open a cursor to perform database operations
cur = conn.cursor(cursor_factory=extras.DictCursor)
# Execute a command: this creates a new table
# cur.execute("CREATE TABLE IF NOT EXISTS test (id serial PRIMARY KEY, num integer, data varchar);")
# Execute a command: this inserts a new record
# cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (200, "JSON"))
# cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (500, "wrc"))

# Execute command to query the table.
cur.execute("SELECT DISTINCT ip, detected_date FROM robot_ip WHERE detected_num > 100;")
# cur.execute("SELECT * FROM human_visits WHERE num_of_visits > 100;")
# After executing the query, need to define a list to put the results in: rows
rows = cur.fetchall()
print(rows)
print(rows[1]['detected_date'])
# if not rows:
#     print('not records')
# else:
#     for row in rows:
#         print(row['detected_date'])

# convert cik to 10-digit string
# row = cur.fetchone()
# print(row)
# cik = float(row[2])
# str_cik = str(int(cik))
# full_cik = str_cik.zfill(10)


# Execute a command: drop a table
# cur.execute("DROP TABLE IF EXISTS test")
# Make the changes to the database persistent
conn.commit()

cur.close()
conn.close()
