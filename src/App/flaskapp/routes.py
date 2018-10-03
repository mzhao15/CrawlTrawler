from flask import render_template, request, flash, url_for
from flaskapp import app
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import json

# parameters for postgres database connection
params = {
    'database': 'my_insight_db',
    'user': 'mzhao15',
    'password': 'zhaomeng148097',
    'host': 'mypqlinstance.cewczr0j0xld.us-east-1.rds.amazonaws.com',
    'port': 5432
}


def dateserializer(realdate):
    ''' serialize datetime.date '''
    if isinstance(realdate, datetime.date):
        return realdate.__str__()


@app.route("/")
@app.route("/home")
def home():
    title = 'Home'
    return render_template('home.html', title=title)


@app.route("/about")
def about():
    title = 'About'
    return render_template('about.html', title=title)


@app.route("/airflow")
def airflow():
    title = 'Airflow'
    return render_template('airflow.html', title=title)


@app.route("/charts")
def charts():
    title = 'Data'
    return render_template('charts.html', title=title)


@app.route("/getdata", methods=["GET", "POST"])
def getdata():
    cik = request.args.get('cik')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    disp = request.args.get('disp_name')
    if '.' not in cik:
        cik += '.0'
    # cik = '1542574.0'
    # start_date = '2016-01-01'
    # end_date = '2016-01-09'
    try:
        conn = psycopg2.connect(**params)
    except Exception as er:
        print("Unable to connect to the database")
        print(str(er))

    cur = conn.cursor(cursor_factory=RealDictCursor)

    if disp == 'total':
        cur.execute("SELECT visit_date, \
                            num_of_visits \
                       FROM total \
                      WHERE cik=%s AND (visit_date BETWEEN %s AND %s) ORDER BY visit_date;",
                    (cik, start_date, end_date))
    elif disp == 'human':
        cur.execute("SELECT visit_date, \
                            num_of_visits \
                       FROM human \
                      WHERE cik=%s AND (visit_date BETWEEN %s AND %s) ORDER BY visit_date;",
                    (cik, start_date, end_date))
    else:
        cur.execute("SELECT t.visit_date AS visit_date, \
                            t.num_of_visits AS total_visits, \
                            h.num_of_visits AS human_visits \
                       FROM total t JOIN human AS h \
                         ON t.cik=h.cik AND t.visit_date=h.visit_date \
                      WHERE t.cik=%s AND (t.visit_date BETWEEN %s AND %s) \
                   ORDER BY visit_date;", (cik, start_date, end_date))
    jsonData = json.dumps(cur.fetchall(), default=dateserializer)
    cur.close()
    conn.close()
    return jsonData
