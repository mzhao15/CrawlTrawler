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
#
#
# @app.route("/map")
# def google_map():
#     title = 'Map'
#     return render_template('map.html', title=title)
#
#
# @app.route("/price")
# def price():
#     title = 'Price'
#     return render_template('price.html', title=title)
#
#
# @app.route("/try")
# def mytest():
#     return "this is a test"
