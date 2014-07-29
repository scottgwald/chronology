#!/usr/bin/env python

import gevent.monkey; gevent.monkey.patch_all()
import gevent.pywsgi
import werkzeug.serving

from jia import app

if __name__ == '__main__':
  werkzeug.serving.run_with_reloader(
    lambda: gevent.pywsgi.WSGIServer(('0.0.0.0', app.config['PORT']),
                                     app).serve_forever())

