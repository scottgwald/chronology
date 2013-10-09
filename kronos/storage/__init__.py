# TODO(usmanm): Eventually we should move stream configuration to ZK to allow
# for online changes and easy synchronization between multiple Kronos instances.
from collections import defaultdict
from importlib import import_module

from kronos.conf import settings
from kronos.core.exceptions import BackendMissing
from kronos.core.exceptions import InvalidStreamName
from kronos.core.validators import validate_stream
from kronos.utils.cache import InMemoryLRUCache

class StorageRouter(object):
  def __init__(self):
    self.backends = dict()
    self.backend_to_read = dict()
    self.default_configuration = (None, dict())
    self.configurations = defaultdict(dict)
    self.stream_to_prefix_cache = InMemoryLRUCache(max_items=1000)
    self.load_backends()
    self.load_configurations()

  def load_backends(self):
    """
    Loads all the backends setup in settings.py.
    """
    for name, backend_settings in settings.storage.iteritems():
      backend_path = 'kronos.storage.backends.%s' % backend_settings['backend']
      backend_module, backend_cls = backend_path.rsplit('.', 1)
      backend_module = import_module(backend_module)

      # Create an instance of the configured backend.
      backend_constructor = getattr(backend_module, backend_cls)
      self.backends[name] = backend_constructor(name, **backend_settings)

  def get_backend(self, name):
    try:
      return self.backends[name]
    except KeyError:
      raise BackendMissing(name)

  def get_backends(self):
    return self.backends.iteritems()

  def get_backend_to_read(self):
    return self.backend_to_read.iteritems()

  def load_configurations(self):
    for prefix, options in settings.streams_to_backends.iteritems():
      self.backend_to_read[prefix] = self.get_backend(options['read_backend'])
      backends = options['backends']
      for backend_name, configuration in backends.iteritems():
        backend = self.get_backend(backend_name)
        self.configurations[prefix][backend] = configuration or {}

  def get_configuration(self, stream, backend):
    return self.backends_to_mutate(stream)[backend]

  def get_matching_prefix(self, stream):
    """
    We look at the stream prefixs configured in stream.yaml and match stream
    to the longest prefix.
    """
    try:
      prefix = self.stream_to_prefix_cache.get(stream)
      if prefix:
        if isinstance(prefix, Exception):
          raise prefix
        return prefix
    except KeyError:
      pass
    try:
      validate_stream(stream)
    except InvalidStreamName, e:
      self.stream_to_prefix_cache[stream] = e
      raise e
    default_prefix = ''
    longest_prefix = default_prefix
    for prefix in self.configurations:
      if prefix == default_prefix:
        continue
      if not stream.startswith(prefix):
        continue
      if len(prefix) <= len(longest_prefix):
        continue
      longest_prefix = prefix
    self.stream_to_prefix_cache.set(stream, longest_prefix)
    return longest_prefix
    
  def backends_to_mutate(self, stream):
    """
    Return all the backends enabled for writing for `stream`.
    """
    return self.configurations[self.get_matching_prefix(stream)]

  def backend_to_retrieve(self, stream):
    """
    Return backend enabled for reading for `stream`.
    """
    stream_prefix = self.get_matching_prefix(stream)
    backend_to_read = self.backend_to_read[stream_prefix]
    return (backend_to_read,
            self.configurations[stream_prefix][backend_to_read])

      
router = StorageRouter()
