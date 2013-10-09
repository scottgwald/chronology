storage = {
  'memory': {
    'backend': 'memory.InMemoryStorage',
    'default_max_items': 50000
  },
}

streams_to_backends = {
  '': {
    'backends': {
      'memory': None
    },
    'read_backend': 'memory'
  }
}

