from flask import Flask
app = Flask(__name__)

app.config['SECRET_KEY'] = '8ee35a081f6be0167def288481460e38'

from flaskapp import routes
