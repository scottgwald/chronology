#!/usr/bin/python
import os

from argparse import ArgumentParser

from metis import app
from metis.utils.spark import create_lib_for_spark_workers
from metis.utils.spark import setup_pyspark

if __name__ == '__main__':
  parser = ArgumentParser(description='Metis HTTP server.')
  parser.add_argument('--debug', action='store_true', help='Debug mode?')
  parser.add_argument('--reload', action='store_true', help='Auto-reload?')
  parser.add_argument('--port', default='8155', help='Port to listen on.')
  parser.add_argument('--config', help='Path of config file to use.')
  args = parser.parse_args()

  if args.config:
    app.config.from_pyfile(os.path.join(os.pardir, args.config))

  debug = args.debug or app.debug
  app.config['DEBUG'] = debug
  port = int(args.port or app.port)
  app.config['PORT'] = port

  setup_pyspark()
  create_lib_for_spark_workers()

  app.run(host='0.0.0.0', port=port, debug=debug, use_reloader=args.reload)
