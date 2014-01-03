import importlib
import re
import sys

from kronos.conf.constants import ServingMode
from uuid import getnode

debug = True
serving_mode = ServingMode.ALL

# Node related settings. `id` should be a unique name that identifies this
# Kronos instance, while `name` is simply a more human readable name.
node = {
  'id':  hex(getnode()), # Unique ID for this Kronos server.
  'greenlet_pool_size': 20, # Greenlet poolsize per process.
  'log_directory': 'logs',
  'cors_whitelist_domains': map(re.compile, ['localhost'])
}

# Stream settings.
# `fields` maps what keys the ID and timestamp should be assigned to in an event
# JSON.
# 
# `format` specifies what a valid stream name looks like. Kronos will use the
# stream name as part of the key that events are stored under for each backend.
# Ensure that each backend that you use accepts patterns defined by `format`.
stream = {
  'fields': {
    'id': '@id',
    'timestamp': '@time'
  },
  'format': re.compile(r'^[a-z0-9\_]+(\.[a-z0-9\_]+)*$', re.I)
}

def configure(configuration_name):
  # Proxy all non-native attributes of the current module and the module
  # at `tests.conf.<configuration_name>` to `kronos.conf.settings`.
  import kronos.conf.settings

  shared_module = sys.modules[__name__]
  for attr in dir(shared_module):
    if attr.startswith('__') or attr == 'patch':
      continue
    setattr(kronos.conf.settings, attr, getattr(shared_module, attr))

  # Do this afterwards in case the patch module wants to override something
  # set in this module.
  patch_module = importlib.import_module('tests.conf.%s' % configuration_name)
  for attr in dir(patch_module):
    if attr.startswith('__'):
      continue
    setattr(kronos.conf.settings, attr, getattr(patch_module, attr))
