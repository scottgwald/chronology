#!/usr/bin/python

import gevent.monkey; gevent.monkey.patch_all()
import kronos.core.monkey; kronos.core.monkey.patch_all()

import gevent.pywsgi

import imp
import logging; logging.basicConfig()
import werkzeug.serving

from argparse import ArgumentParser

from kronos.conf import settings
from kronos.conf.constants import ServingMode

if __name__ == '__main__':
  parser = ArgumentParser(description='Kronos HTTP server.')
  parser.add_argument('--debug', action='store_true', help='Debug mode?')
  parser.add_argument('--reload', action='store_true', help='Auto-reload?')
  parser.add_argument('--port', action='store', default='8150',
                      help='port to listen for incoming requests')
  parser.add_argument('--serving-mode',
                      choices=[ServingMode.ALL, ServingMode.COLLECTOR,
                               ServingMode.READONLY],
                      help='which serving mode to run in')
  parser.add_argument('--config', action='store',
                      help='path of config file to use')
  parser.add_argument('--profile', action='store_true',
                      help='Profile each request using cProfile?')
  args = parser.parse_args()

  settings.clear()
  if args.config:
    # If a config file path is given, import that as the `settings` module.  
    settings.update(imp.load_source('kronos.conf.run_settings', args.config))
  else:
    # Otherwise use default settings. This is to ensure we never try to read
    # the settings for the configured kronos service when using this runner
    # script.
    from kronos.conf import default_settings
    settings.update(default_settings)

  # Override the `debug` in the settings module and `debug` for
  # `args`.
  settings.debug = args.debug or settings.debug
  settings.serving_mode = args.serving_mode or settings.serving_mode
  settings.profile = args.profile or settings.profile

  # Only load the application after we've overwritten settings.serving_mode, or
  # else the endpoint access control logic will kick in too early.
  from kronos.app import application

  if args.reload:
    def reload():
      print 'Reloading kronosd...'
      gevent.pywsgi.WSGIServer(('0.0.0.0', int(args.port)),
                               application).serve_forever()
    werkzeug.serving.run_with_reloader(reload)
  else:
    gevent.pywsgi.WSGIServer(('0.0.0.0', int(args.port)),
                             application).serve_forever()
