#!/usr/bin/python

import gevent.monkey; gevent.monkey.patch_all()
import gevent.pywsgi
import geventhttpclient.httplib; geventhttpclient.httplib.patch()
import logging
import multiprocessing
import sys
import werkzeug.serving

from argparse import ArgumentParser

from kronos.server import GunicornApplication, wsgi_application

if __name__ == "__main__":
  parser = ArgumentParser(description='Kronos HTTP server.')
  parser.add_argument('--debug', action='store_true', help='run in debug mode?')
  parser.add_argument('--host', action='store', help='incoming requests host',
                      default='0.0.0.0')
  parser.add_argument('--port', action='store', help='incoming requests port',
                      default=8150)
  parser.add_argument('--num_proc', action='store', default=0,
                      help='number of processes to run')
  args = parser.parse_args()
  if args.debug:
    werkzeug.serving.run_with_reloader(
        lambda: gevent.pywsgi.WSGIServer((args.host, int(args.port)),
                                         wsgi_application).serve_forever())
  else:
    # Clear command line arguments, so that GunicornApplication doesn't try to 
    # parse them.
    del sys.argv[1:]
    GunicornApplication({
        'bind': '%s:%s' % (args.host, int(args.port)),
        'workers': int(args.num_proc) or multiprocessing.cpu_count() * 2 + 1,
        }).run()
