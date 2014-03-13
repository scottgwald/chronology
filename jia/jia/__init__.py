from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config.from_pyfile('../settings.cfg')
db = SQLAlchemy(app)

import jia.models
import jia.views

# Create tables in sqlite3 db if not present.
db.create_all()
