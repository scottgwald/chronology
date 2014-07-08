from collections import defaultdict
from importlib import import_module

from kronos.common.cache import memoize
from kronos.common.lazy import LazyObjectMetaclass
from kronos.common.settings import merge_dicts
from kronos.conf import settings
from kronos.core.exceptions import BackendMissing
from kronos.core.exceptions import NamespaceMissing
from kronos.core.validators import validate_stream


class StorageRouter(object):
  # Make the StorageRouter be a lazily instantiated object. That way doing:
  # >  from kronos.storage.router import router
  # will not cause the router to be initialized. This is needed when we run
  # kronos as a service, because uwsgi uses a pre-fork model and if router
  # is instantiated before the fork, then all worker processes will try to share
  # connection objects for storage engines. That would work fine for storage
  # engines that only rely on HTTP calls, but engines like Cassandra use sockets
  # which cannot be shared between worker processes.
  __metaclass__ = LazyObjectMetaclass
  
  def __init__(self):
    self.backends = dict()
    self.prefix_read_backends = dict()
    self.prefix_confs = dict()
    self.namespaces = settings.namespace_to_streams_configuration.keys()
    self.load_backends()
    self.load_prefix_configurations()

  def reload(self):
    self.__init__()

  def load_backends(self):
    """
    Loads all the backends setup in settings.py.
    """
    for name, backend_settings in settings.storage.iteritems():
      backend_path = 'kronos.storage.%s' % backend_settings['backend']
      backend_module, backend_cls = backend_path.rsplit('.', 1)
      backend_module = import_module(backend_module)
      # Create an instance of the configured backend.
      backend_constructor = getattr(backend_module, backend_cls)
      self.backends[name] = backend_constructor(name,
                                                namespaces=self.namespaces,
                                                **backend_settings)

  def get_backend(self, name):
    try:
      return self.backends[name]
    except KeyError:
      raise BackendMissing(name)

  def get_backends(self):
    return self.backends.iteritems()

  def get_read_backends(self, namespace):
    return self.prefix_read_backends[namespace].iteritems()

  def load_prefix_configurations(self):
    for namespace, streams_conf in (settings.namespace_to_streams_configuration
                                    .iteritems()):
      prefix_read_backends = self.prefix_read_backends[namespace] = dict()
      prefix_confs  = self.prefix_confs[namespace] = defaultdict(dict)
      for prefix, options in streams_conf.iteritems():
        prefix_read_backends[prefix] = self.get_backend(options['read_backend'])
        backends = options['backends']
        for backend_name, configuration in backends.iteritems():
          backend = self.get_backend(backend_name)
          prefix_confs[prefix][backend] = merge_dicts(backend._settings,
                                                      configuration or {})

  def get_namespaces(self):
    return self.namespaces

  def get_configuration(self, namespace, stream, backend):
    return self.backends_to_mutate(namespace, stream)[backend]

  @memoize(max_items=2000)
  def get_matching_prefix(self, namespace, stream):
    """
    We look at the stream prefixs configured in stream.yaml and match stream
    to the longest prefix.
    """
    validate_stream(stream)
    default_prefix = ''
    longest_prefix = default_prefix
    for prefix in self.prefix_confs[namespace]:
      if prefix == default_prefix:
        continue
      if not stream.startswith(prefix):
        continue
      if len(prefix) <= len(longest_prefix):
        continue
      longest_prefix = prefix
    return longest_prefix
    
  def backends_to_mutate(self, namespace, stream):
    """
    Return all the backends enabled for writing for `stream`.
    """
    if namespace not in self.namespaces:
      raise NamespaceMissing('`{}` namespace is not configured'
                             .format(namespace))
    return self.prefix_confs[namespace][self.get_matching_prefix(namespace,
                                                                 stream)]

  def backend_to_retrieve(self, namespace, stream):
    """
    Return backend enabled for reading for `stream`.
    """
    if namespace not in self.namespaces:
      raise NamespaceMissing('`{}` namespace is not configured'
                             .format(namespace))
    stream_prefix = self.get_matching_prefix(namespace, stream)
    read_backend = self.prefix_read_backends[namespace][stream_prefix]
    return (read_backend,
            self.prefix_confs[namespace][stream_prefix][read_backend])

      
router = StorageRouter()
