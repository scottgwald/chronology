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
  parser.add_argument('--bind', action='store', default='0.0.0.0:8150',
                      help='hostname:port or unix:/path to listen for incoming requests')
  parser.add_argument('--num_proc', action='store',
                      default=multiprocessing.cpu_count() * 2 + 1,
                      help='number of processes to run')
  args = parser.parse_args()
  if args.debug:
    (host, port) = args.bind.split(':')
    werkzeug.serving.run_with_reloader(
        lambda: gevent.pywsgi.WSGIServer((host, int(port)),
                                         wsgi_application).serve_forever())
  else:
    # Clear command line arguments, so that GunicornApplication doesn't try to 
    # parse them.
    del sys.argv[1:]
    GunicornApplication({'bind': args.bind, 'workers': int(args.num_proc)}).run()
