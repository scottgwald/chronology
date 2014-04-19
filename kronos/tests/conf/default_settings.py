import re

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
