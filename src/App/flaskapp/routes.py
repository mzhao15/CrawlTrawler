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


@app.route("/charts")
def charts():
    title = 'Charts'
    return render_template('charts.html', title=title)
