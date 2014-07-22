import logging
import os

from kronos.conf import settings

def configure():
  log_dir = settings.node.log_directory.rstrip('/')
  if not os.path.exists(log_dir):
    os.makedirs(log_dir)
  # TODO(usmanm): Separate out error.log
  logging.basicConfig(filename='%s/app.log' % log_dir,
                      filemode='a',
                      level=logging.NOTSET,
                      format='%(asctime)s:%(levelname)s:%(message)s')

  # elasticsearch-py is annoying. They turn propagate=False for the following
  # logger so we keep on seeing 'No handlers could be found for logger
  # "elasticsearch.trace"'. Fix that by piping it to NullHandler.
  logging.getLogger('elasticsearch.trace').addHandler(logging.NullHandler())
  
