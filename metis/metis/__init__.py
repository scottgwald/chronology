import os

VERSION = (0, 1, 'alpha')

# The file path will have `metis.zip` in it if its being run on Spark workers.
# In that case we don't want to run the following initialization code because
# it can (and does) break things.
if 'metis.zip' not in str(__file__):
  from flask import Flask

  METIS_PATH = os.path.realpath(os.path.dirname(__file__))

  app = Flask(__name__)
  app.config.from_pyfile('%s/conf/settings.py' % METIS_PATH)
  app.config['PATH'] = METIS_PATH

  import metis.views
else:
  app = None
