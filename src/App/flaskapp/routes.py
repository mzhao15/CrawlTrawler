from flask import render_template  # , url_for, flash, redirect
from flaskapp import app


@app.route("/")
@app.route("/home")
def home():
    imgname = 'jhu.jpg'
    title = 'Home'
    return render_template('home.html', title=title, imgname=imgname)


@app.route("/search")
def search():
    title = 'Search'
    return render_template('search.html', title=title)


@app.route("/map")
def google_map():
    title = 'Map'
    return render_template('map.html', title=title)


@app.route("/price")
def price():
    title = 'Price'
    return render_template('price.html', title=title)


@app.route("/try")
def mytest():
    return "this is a test"
