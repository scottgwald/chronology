from kronos.utils import aws

# Backends.
storage = {
    #'cassandra_timewidth': {
    #'backend': 'cassandra.TimeWidthCassandraStorage',
    #'hosts': ['127.0.0.1:9160'],
    #'keyspace': 'kronos_tw_dev',
    #'replication_factor': 1,
    #'default_timewidth_seconds': 86400,
    #'default_shards_per_bucket': 3,
    #'read_size': 5000
    #},
  'memory': {
    'backend': 'memory.InMemoryStorage',
    'default_max_items': 100000
    },
  }

# Node related settings. `id` should be a unique name that identifies this
# Kronos instance, while `name` is simply a more human readable name.
node = {
  'id':  aws.get_instance_id(),
  'greenlet_pool_size': 25,
  'log_directory': 'log'
  }

# Stream settings. `fields` maps what keys the ID and timestamp should be
# assigned to in an event JSON.
stream = {
  'fields': {
    'id': '@id',
    'timestamp': '@time'
    }
  }

# TODO(usmanm): Add configuration for logging events for Kronos itself.
