#!/usr/bin/python

import gevent.monkey; gevent.monkey.patch_all()
import gevent.pywsgi
import geventhttpclient.httplib; geventhttpclient.httplib.patch()

import multiprocessing
import subprocess
import werkzeug.serving
import os

from argparse import ArgumentParser

from kronos.conf import settings
from kronos.server import wsgi_application

if __name__ == '__main__':
  parser = ArgumentParser(description='Kronos HTTP server.')
  parser.add_argument('--debug', action='store_true', help='debug mode?')
  parser.add_argument('--bind', action='store', default='0.0.0.0:8150',
                      help='hostname:port or unix:/path to listen for '
                      'incoming requests')
  parser.add_argument('--num-proc', action='store',
                      default=multiprocessing.cpu_count() * 2 + 1,
                      help='number of processes to run')
  parser.add_argument('--collector-mode', action='store_true',
                      help='only open the put endpoint?')
  args = parser.parse_args()

  # Copy over some args to settings.py.
  for arg in ('debug', 'collector_mode'):
    setattr(settings, arg, getattr(args, arg))
  
  if args.debug:
    (host, port) = args.bind.split(':')
    werkzeug.serving.run_with_reloader(
        lambda: gevent.pywsgi.WSGIServer((host, int(port)),
                                         wsgi_application).serve_forever())
  else:
    # Try creating log directory, if missing.
    log_dir = settings.node['log_directory'].rstrip('/')
    if not os.path.exists(log_dir):
      os.makedirs(log_dir)

    argv = ['gunicorn', 'kronos.server:wsgi_application',
            '--worker-class', 'gevent',
            '--log-level', 'info',
            '--name', 'kronosd',
            '--access-logfile', '{0}/{1}'.format(log_dir, 'access.log'),
            '--error-logfile', '{0}/{1}'.format(log_dir, 'error.log'),
            '--workers', str(args.num_proc),
            '--bind', args.bind]
    subprocess.call(argv)
