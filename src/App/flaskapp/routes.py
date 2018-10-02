from flask import render_template, url_for, redirect, request, jsonify
from flaskapp import app
import json
import psycopg2


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

@app.route("/getdata", methods=["GET","POST"])
def getdata():
	params = {
	    'database': 'my_insight_db',
	    'user': 'mzhao15',
	    'password': 'zhaomeng148097',
	    'host': 'mypqlinstance.cewczr0j0xld.us-east-1.rds.amazonaws.com',
	    'port': 5432
	}
	cik = request.args.get('cik')
	start_date = request.args.get('start_date')
	end_date = request.args.get('end_date')

	try:
	    conn = psycopg2.connect(**params)
	except Exception as er1:
	    print("I am unable to connect to the database")
	    print(str(er1))

	cur = conn.cursor(cursor_factory=extras.DictCursor)
	cur.execute("SELECT visit_date, cik, num_of_visits FROM total WHERE cik=%s AND (visit_date BETWEEN %s AND %s);",(cik, start_date, end_date))
	data = cur.fetchall()

	return jsonify(data)
