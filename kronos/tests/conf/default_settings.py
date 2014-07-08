import re

from kronos.conf.constants import ServingMode
from uuid import getnode

debug = True
serving_mode = ServingMode.ALL

storage = {
  'memory': {
    'backend': 'memory.InMemoryStorage',
    'max_items': 50000
  },
  'cassandra': {
    'backend': 'cassandra.CassandraStorage',
    'hosts': ['127.0.0.1'],
    'keyspace_prefix': 'kronos_test',
    # Set to a value greater than 0 or you will get an UnavailableException
    'replication_factor': 1,
    'timewidth_seconds': 2, # Keep this small for test environment.
    'shards_per_bucket': 3,
    'read_size': 10
  },
  'elasticsearch': {
    'backend': 'elasticsearch.ElasticSearchStorage',
    'hosts': [{'host': 'localhost',
               'port': 9200}],
    'keyspace_prefix': 'kronos_test',
    'event_index_template': 'kronos',
    'event_index_prefix': 'kronos',
    'force_refresh': True,
    # TODO(usmanm): Support for the following  configs will be added soon.
    'rollover_size': 10000, #?
    'rollover_check_period_seconds': 10, #?
    'read_size': 5000 #?
  }
}

node = {
  'id':  hex(getnode()),
  'greenlet_pool_size': 50,
  'gipc_pool_size': 2,
  'log_directory': 'logs',
  'cors_whitelist_domains': map(re.compile, ['localhost'])
}

stream = {
  'fields': {
    'id': '@id',
    'timestamp': '@time'
  },
  'format': re.compile(r'^[a-z0-9\_]+(\.[a-z0-9\_]+)*$', re.I)
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
  'namespace1': _default_stream_configuration,
  'namespace2': _default_stream_configuration
  }
