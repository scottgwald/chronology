import re

from kronos.conf.constants import ServingMode
from uuid import getnode

debug = True
profile = False
serving_mode = ServingMode.ALL

storage = {
  'cassandra': {
    'backend': 'cassandra.CassandraStorage',
    'hosts': ['127.0.0.1'],
    'keyspace_prefix': 'kronos_benchmark',
    # Set to a value greater than 0 or you will get an UnavailableException
    'replication_factor': 1,
    'timewidth_seconds': 1000, # Keep this small for test environment.
    'shards_per_bucket': 1,
    'read_size': 5000
  }
}

node = {
  'id':  hex(getnode()),
  'flush_size': 131072,
  'greenlet_pool_size': 50,
  'gipc_pool_size': 2,
  'log_directory': 'logs',
  'cors_whitelist_domains': map(re.compile, ['localhost'])
}

stream = {
  'format': re.compile(r'^[a-z0-9\_]+(\.[a-z0-9\_]+)*$', re.I)
}

default_namespace = 'kronos'

namespace_to_streams_configuration = {
  default_namespace: {
    '': {
      'backends': {
        'cassandra': None
        },
      'read_backend': 'cassandra'
      },
    'shards_3': {
      'backends': {
        'cassandra': {'shards_per_bucket': 3}
        },
      'read_backend': 'cassandra'
      },
    'shards_6': {
      'backends': {
        'cassandra': {'shards_per_bucket': 6}
        },
      'read_backend': 'cassandra'
      },
    'shards_12': {
      'backends': {
        'cassandra': {'shards_per_bucket': 12}
        },
      'read_backend': 'cassandra'
      },
    'shards_24': {
      'backends': {
        'cassandra': {'shards_per_bucket': 24}
        },
      'read_backend': 'cassandra'
      }
    }
  }
