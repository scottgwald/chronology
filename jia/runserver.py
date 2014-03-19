#!/usr/bin/python

import gevent.monkey; gevent.monkey.patch_all()
import gevent.pywsgi
import werkzeug.serving

from jia import app

if __name__ == '__main__':
  app.config.from_pyfile('../settings.cfg')
  werkzeug.serving.run_with_reloader(
    lambda: gevent.pywsgi.WSGIServer(('0.0.0.0', 8153),
                                     app).serve_forever())
