import re

from kronos.conf.constants import ServingMode
from uuid import getnode

debug = True
profile = False
serving_mode = ServingMode.ALL

node = {
  'id':  hex(getnode()),
  'flush_size': 512,
  'greenlet_pool_size': 20,
  'gipc_pool_size': 1,
  'log_directory': 'logs',
  'cors_whitelist_domains': map(re.compile, ['localhost'])
}

stream = {
  'format': re.compile(r'^[a-z0-9\_]+(\.[a-z0-9\_]+)*$', re.I)
}

storage = {
  'memory': {
    'backend': 'memory.InMemoryStorage',
    'max_items': 50000
  },
}

default_namespace = 'kronos'

_default_stream_configuration = {
  '': {
    'backends': {
      'memory': None
      },
    'read_backend': 'memory'
    }
  }

namespace_to_streams_configuration = {
  default_namespace: _default_stream_configuration,
  }
