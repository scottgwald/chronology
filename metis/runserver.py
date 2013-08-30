#!/usr/bin/python
from gevent.wsgi import WSGIServer
from metis import app

if app.debug:
  app.run(host='0.0.0.0', port=8152)
else:
  WSGIServer(('0.0.0.0', 8152), app).serve_forever()

