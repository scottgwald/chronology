# TODO(usmanm): Eventually we should move stream configuration to ZK to allow
# for online changes and easy synchronization between multiple Kronos instances.

import re
import yaml

from collections import defaultdict
from importlib import import_module

from kronos.conf import settings
from kronos.core.exceptions import (BackendMissing, ImproperlyConfigured,
                                    InvalidStreamName)
from kronos.core.validators import validate_stream

MATCH_PATTERN = '*'

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
    return re.compile('%s%s%s' %
                      ('^',
                       (stream_pattern
                        .replace('.', '\.')
                        .replace(MATCH_PATTERN, '(.*)')),
                       '$'), re.I)

  def regex_to_pattern(self, stream_regex):
    return stream_regex.pattern[1:-1].replace('\.', '.').replace('(.*)',
                                                                 MATCH_PATTERN)
  
  def load_backends(self):
    """
    Loads all the backends setup in settings.py.
    """
    backends_settings = getattr(settings, 'storage', {})
    if not isinstance(backends_settings, dict):
      raise ImproperlyConfigured('`storage` setting must be a dict.')
    if not backends_settings:
      raise ImproperlyConfigured('Must define at least one backend in the '
                                 '`storage` settings')
    backends = {}
    for name, backend_settings in backends_settings.iteritems():
      if not (isinstance(backend_settings, dict) and
              'backend' in backend_settings):
        raise ImproperlyConfigured('`storage.%s` setting must be a valid dict '
                                   'and contain a `backend` key.' % name)
      backend_path = ('kronos.storage.backends.%s' %
                      backend_settings.get('backend'))
      backend_module, backend_cls = backend_path.rsplit('.', 1)
      backend_module = import_module(backend_module)
      # Create an instance of the configured backend.
      backends[name] = (getattr(backend_module, backend_cls)
                        (name, **backend_settings))
    self.backends = backends

  def get_backend(self, name):
    try:
      return self.backends[name]
    except KeyError:
      raise BackendMissing(name)

  def get_backends(self):
    return self.backends.iteritems()

  def load_configurations(self):
    configurations = yaml.load(open('kronos/conf/streams.yaml'))
    # Validate stream configurations.
    for stream_pattern, options in configurations.iteritems():
      # Validate stream pattern by replacing '*' with any legal string.
      validate_stream(stream_pattern.replace(MATCH_PATTERN, 'jia'))
      stream_regex = self.pattern_to_regex(stream_pattern)
      backend_to_read = options.get('read_backend')
      if not backend_to_read:
        raise ImproperlyConfigured('Must provide `read_backend` for stream '
                                   'pattern `%s`.' % stream_pattern)      
      self.backend_to_read[stream_regex] = self.get_backend(backend_to_read)
      backends = options.get('backends')
      if not backends:
        raise ImproperlyConfigured('Must configure at least one `backends` for '
                                   'stream pattern `%s`.' % stream_pattern)
      if backend_to_read not in backends:
        raise ImproperlyConfigured('`backend_to_read` missing from `backends` '
                                   'for %s.' % stream_pattern)
      for backend_name, configuration in backends.iteritems():
        if configuration is None:
          configuration = {}
        backend = self.get_backend(backend_name)
        backend.validate_configuration(configuration)
        self.configurations[stream_regex][backend] = configuration
        
    if not self.pattern_to_regex(MATCH_PATTERN) in self.configurations:
      raise ImproperlyConfigured('Must configure stream pattern `%s`.' %
                                 MATCH_PATTERN)

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
    default_regex = self.pattern_to_regex(MATCH_PATTERN)
    best_match = (default_regex, float('inf'), -float('inf'))
    for stream_pattern in self.configurations:
      if stream_pattern == default_regex:
        continue
      match = stream_pattern.match(stream)
      if match:
        literal_pattern = self.regex_to_pattern(stream_pattern)
        extra_dots = stream.count('.') - literal_pattern.count('.')
        literal_count = len(literal_pattern.replace(MATCH_PATTERN, ''))
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
