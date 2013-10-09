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

streams_to_backends = {
  '': {
    'backends': {
      'cassandra_timewidth': None
    },
    'read_backend': 'cassandra_timewidth'
  }
}
