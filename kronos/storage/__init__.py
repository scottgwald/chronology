# TODO(usmanm): Eventually we should move stream configuration to ZK to allow
# for online changes and easy synchronization between multiple Kronos instances.

import re

from collections import defaultdict
from importlib import import_module

from kronos.conf import settings
from kronos.core.exceptions import BackendMissing
from kronos.core.exceptions import InvalidStreamName
from kronos.core.validators import validate_stream

class StorageRouter(object):
  def __init__(self):
    self.backends = dict()
    self.backend_to_read = dict()
    self.default_configuration = (None, dict())
    self.configurations = defaultdict(dict)
    self.stream_to_pattern_cache = dict()
    self.load_backends()
    self.load_configurations()

  def pattern_to_regex(self, stream_pattern):
    return re.compile('^%s$' % (stream_pattern
                                  .replace('.', '\.')
                                  .replace('*', '(.*)'))
                     , re.I)

  def regex_to_pattern(self, stream_regex):
    return (stream_regex.pattern[1:-1]
              .replace('\.', '.')
              .replace('(.*)', '*'))
  
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

  def load_configurations(self):
    for pattern, options in settings.streams_to_backends.iteritems():
      regex = self.pattern_to_regex(pattern)
      self.backend_to_read[regex] = self.get_backend(options['read_backend'])
      backends = options['backends']
      for backend_name, configuration in backends.iteritems():
        backend = self.get_backend(backend_name)
        self.configurations[regex][backend] = configuration or {}

  def get_configuration(self, stream, backend):
    return self.backends_to_insert(stream)[backend]

  def get_matching_pattern(self, stream):
    """
    We look at the stream patterns configured in stream.yaml and match stream
    to the closest pattern. The closest pattern is defined as the pattern which
    matches the most `.` in `stream`. The length of the overlap between `stream`
    and the pattern is used as a tie breaker.
    """
    pattern = self.stream_to_pattern_cache.get(stream)
    if pattern:
      if isinstance(pattern, Exception):
        raise pattern
      return pattern
    try:
      validate_stream(stream)
    except InvalidStreamName, e:
      self.stream_to_pattern_cache[stream] = e
      raise e
    default_regex = self.pattern_to_regex('*')
    best_match = (default_regex, float('inf'), -float('inf'))
    for stream_pattern in self.configurations:
      if stream_pattern == default_regex:
        continue
      match = stream_pattern.match(stream)
      if match:
        literal_pattern = self.regex_to_pattern(stream_pattern)
        extra_dots = stream.count('.') - literal_pattern.count('.')
        literal_count = len(literal_pattern.replace('*', ''))
        if (extra_dots < best_match[1] or
            (extra_dots == best_match[1] and literal_count > best_match[2])):
          best_match = (stream_pattern, extra_dots, literal_count)
    self.stream_to_pattern_cache[stream] = best_match[0]
    return best_match[0]
    
  def backends_to_insert(self, stream):
    """
    Return all the backends enabled for writing for `stream`.
    """
    return self.configurations[self.get_matching_pattern(stream)]

  def backend_to_retrieve(self, stream):
    """
    Return backend enabled for reading for `stream`.
    """
    stream_pattern = self.get_matching_pattern(stream)
    backend_to_read = self.backend_to_read[stream_pattern]
    return (backend_to_read,
            self.configurations[stream_pattern][backend_to_read])
      
router = StorageRouter()
