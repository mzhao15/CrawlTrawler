from flask import render_template, request, flash, url_for, redirect
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
    ''' serialize datetime.date() '''
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
    try:
        conn = psycopg2.connect(**params)
    except Exception as er:
        print("Unable to connect to the database")
        print(str(er))

    cur = conn.cursor(cursor_factory=RealDictCursor)

    if disp == 'total':
        # get data from table 'total'
        cur.execute("SELECT visit_date, \
                            num_of_visits \
                       FROM total \
                      WHERE cik=%s AND (visit_date BETWEEN %s AND %s) \
                   ORDER BY visit_date;", (cik, start_date, end_date))
    elif disp == 'human':
        # get data from table 'human'
        cur.execute("SELECT visit_date, \
                            num_of_visits \
                       FROM human \
                      WHERE cik=%s AND (visit_date BETWEEN %s AND %s) \
                   ORDER BY visit_date;", (cik, start_date, end_date))
    else:
        # get joint data from table 'total' and 'human'
        cur.execute("SELECT t.visit_date AS visit_date, \
                            h.num_of_visits AS human_visits, \
                            t.num_of_visits-h.num_of_visits AS robot_visits \
                       FROM total t JOIN human h \
                         ON t.cik=h.cik AND t.visit_date=h.visit_date \
                      WHERE t.cik=%s AND (t.visit_date BETWEEN %s AND %s) \
                   ORDER BY visit_date;", (cik, start_date, end_date))

    raw = cur.fetchall()
    # data type: json
    jsonData = json.dumps(raw, default=dateserializer)
    cur.close()
    conn.close()
    return jsonData


@app.route("/getairflow", methods=["GET", "POST"])
def getairflow():
    dagdate = request.args.get('dagdate')
    taskname = request.args.get('taskname')
    response = {"dagdate": dagdate, "taskname": taskname}
    try:
        conn = psycopg2.connect(**params)
    except Exception as er:
        print("Unable to connect to the database")
        print(str(er))
    cur = conn.cursor(cursor_factory=RealDictCursor)

    markfinding = 0
    markcounting = 0
    cur.execute("SELECT id \
                   FROM human \
                  WHERE visit_date=%s AND cik=%s", (dagdate, "1542574.0"))
    raw = cur.fetchall()
    if raw:
        markcounting = 1
        markfinding = 1

    if taskname == 'finding':
        if markcounting == 0:
            cur.execute("SELECT id \
                           FROM robot_ip \
                          WHERE visit_date=%s AND cik=%s", (dagdate, "1542574.0"))
            raw = cur.fetchall()
            if raw:
                markfinding = 1

    if taskname == 'finding':
        if markfinding == 1:
            response["status"] = "completed"
        else:
            response["status"] = "not completed"
    else:
        if markcounting == 1:
            response["status"] = "completed"
        else:
            response["status"] = "not completed"
    cur.close()
    conn.close()
    return json.dumps(response)
