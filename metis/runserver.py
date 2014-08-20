#!/usr/bin/python
import os

from argparse import ArgumentParser

from metis import app


if __name__ == '__main__':
  parser = ArgumentParser(description='Metis HTTP server.')
  parser.add_argument('--debug', action='store_true', help='Debug mode?')
  parser.add_argument('--reload', action='store_true', help='Auto-reload?')
  parser.add_argument('--port', default='8152', help='Port to listen on.')
  parser.add_argument('--config', help='Path of config file to use.')
  args = parser.parse_args()

  if args.config:
    app.config.from_pyfile(os.path.join(os.pardir, args.config))

  debug = args.debug or app.debug
  app.config['DEBUG'] = debug
  port = int(args.port)
  app.config['PORT'] = port

  app.run(host='0.0.0.0', port=port, debug=debug, use_reloader=args.reload)
