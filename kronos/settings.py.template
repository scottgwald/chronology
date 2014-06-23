import re

from uuid import getnode

from kronos.conf.constants import ServingMode

debug = False
serving_mode = ServingMode.ALL

# Backends.
storage = {
  'memory': {
    'backend': 'memory.InMemoryStorage',
    'max_items': 1000
  },
}

# Node related settings. `id` should be a unique name that identifies this
# Kronos instance, while `name` is simply a more human readable name.
node = {
  'id':  hex(getnode()), # Unique ID for this Kronos server.
  'greenlet_pool_size': 20, # Greenlet poolsize per process.
  'log_directory': 'logs',
  'cors_whitelist_domains' : map(re.compile, [
    # Domains that match any regex in this list will be allowed to talk to this
    # Kronos instance
  ])
}

# Stream settings.
# `format` specifies what a valid stream name looks like. Kronos will use the
# stream name as part of the key that events are stored under for each backend.
# Ensure that each backend that you use accepts patterns defined by `format`.
stream = {
  'format': re.compile(r'^[a-z0-9\_]+(\.[a-z0-9\_]+)*$', re.I)
}

# Default namespace for streams.
default_namespace = 'kronos'

# Maps namespace => { stream_prefix => { backends => {}, read_backend => {} } }
namespace_to_streams_configuration = {
  default_namespace: {
    '': {
      'backends': {
        'memory': None
        },
      'read_backend': 'memory'
      }
    }
  }

# TODO(usmanm): Add configuration for logging events for Kronos itself.
