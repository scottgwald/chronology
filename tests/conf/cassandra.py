storage = {
  'cassandra_timewidth': {
    'backend': 'cassandra.TimeWidthCassandraStorage',
    'hosts': ['localhost:9160'],
    'keyspace': 'kronos_tw_test',
    'replication_factor': 1,
    'default_timewidth_seconds': 2, # Keep this small for test environment.
    'default_shards_per_bucket': 3
  }
}

default_namespace = 'kronos'

_default_stream_configuration = {
  '': {
    'backends': {
      'cassandra': None
      },
    'read_backend': 'cassandra'
    }
  }

namespace_to_streams_configuration = {
  default_namespace: _default_stream_configuration,
  'namespace1': _default_stream_configuration,
  'namespace2': _default_stream_configuration
  }
