#!/usr/bin/python
from gevent.wsgi import WSGIServer

from metis import app
from metis.utils.spark import create_lib_for_spark_workers
from metis.utils.spark import setup_pyspark

setup_pyspark()
create_lib_for_spark_workers()

if app.debug:
  app.run(host='0.0.0.0', port=app.config['PORT'])
else:
  WSGIServer(('0.0.0.0', app.config['PORT']), app).serve_forever()

