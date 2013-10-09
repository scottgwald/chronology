storage = {
  'memory': {
    'backend': 'memory.InMemoryStorage',
    'default_max_items': 50000
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
  'namespace1': _default_stream_configuration,
  'namespace2': _default_stream_configuration
  }
