from flask import render_template, url_for, redirect
from flaskapp import app


@app.route("/")
@app.route("/home")
def home():
    title = 'Home'
    return render_template('home.html', title=title)


@app.route("/about")
def about():
    title = 'About'
    return render_template('about.html', title=title)


@app.route("/spark")
def spark():
    title = 'Spark'
    return render_template('spark.html', title=title)
