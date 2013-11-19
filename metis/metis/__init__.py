import os

from flask import Flask

METIS_PATH = os.path.realpath(os.path.dirname(__file__))

app = Flask(__name__)
app.config.from_pyfile('%s/conf/settings.py' % METIS_PATH)
app.config['PATH'] = METIS_PATH

VERSION = (0, 1, 'alpha')

import metis.views
