storage = {
    'elasticsearch' : {
      'backend' : 'elasticsearch.ElasticSearchStorage',
      'hosts' : [
        {
          'host' : 'localhost',
          'port' : 9200
          },
      ],
      'keyspace_prefix' : 'kronos_test',
      'event_index_template': 'kronos',
      'event_index_prefix' : 'kronos',
      'rollover_size' : 10000, #?
      'rollover_check_period_seconds' : 10, #?
      'read_size' : 5000, #?
      'default_max_items' : 1000, #rm
      'force_refresh': True,
      }
}

default_namespace = 'kronos'

_default_stream_configuration = {
      '': {
            'backends': {
                    'elasticsearch': None
                          },
                'read_backend': 'elasticsearch'
                    }
        }

namespace_to_streams_configuration = {
  default_namespace: _default_stream_configuration,
  'namespace1': _default_stream_configuration,
  'namespace2': _default_stream_configuration
}
