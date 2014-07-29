import json

from flask import Flask, request
from flask.ext.sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config.from_pyfile('../settings.cfg')
app.config.update(PORT=app.config['SCHEDULER_PORT'])
app.config.update(SQLALCHEMY_DATABASE_URI=app.config['SCHEDULER_DATABASE_URI'])
app.secret_key = app.config['SECRET_KEY']
db = SQLAlchemy(app)

import models
import views

# Create tables in sqlite3 db if not present.
db.create_all()
