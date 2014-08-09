import re

from kronos.conf.constants import ServingMode
from uuid import getnode

debug = True
profile = False
serving_mode = ServingMode.ALL

storage = {
  'elasticsearch': {
    'backend': 'elasticsearch.ElasticSearchStorage',
    'hosts': [{'host': 'localhost', 'port': 9200}],
    'index_template': 'kronos_bench',
    'index_prefix': 'kronos_bench',
    'shards': 5,
    'replicas': 0,
    'force_refresh': False,
    'read_size': 5000,
    'rollover_size': 10000000,
    'rollover_check_period_seconds': 15
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
        'elasticsearch': None
        },
      'read_backend': 'elasticsearch'
      },
    'read_size_10000': {
      'backends': {
        'elasticsearch': {'read_size': 10000}
        },
      'read_backend': 'elasticsearch'
      },
    'read_size_15000': {
      'backends': {
        'elasticsearch': {'read_size': 15000}
        },
      'read_backend': 'elasticsearch'
      },
    'read_size_20000': {
      'backends': {
        'elasticsearch': {'read_size': 20000}
        },
      'read_backend': 'elasticsearch'
      }

    }
  }
