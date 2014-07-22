from __future__ import absolute_import

import logging
import os
import sys

from kronos.conf import settings

def configure():
  log_dir = settings.node.log_directory.rstrip('/')
  if not os.path.exists(log_dir):
    os.makedirs(log_dir)

  formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

  if settings.debug:
    app_handler = logging.StreamHandler(sys.stdout)
    error_handler = logging.StreamHandler(sys.stderr)
  else:
    app_handler = logging.FileHandler('%s/app.log' % log_dir)
    error_handler = logging.FileHandler('%s/error.log' % log_dir)

  app_handler.setLevel(logging.NOTSET)
  app_handler.setFormatter(formatter)
  error_handler.setLevel(logging.ERROR)
  error_handler.setFormatter(formatter)

  logging.root.addHandler(error_handler)
  logging.root.addHandler(app_handler)

  # XXX: elasticsearch-py is annoying. They turn propagate=False for the
  # following logger so we keep on seeing 'No handlers could be found for logger
  # "elasticsearch.trace"'. Fix that by piping it to NullHandler.
  logging.getLogger('elasticsearch.trace').addHandler(logging.NullHandler())
  
