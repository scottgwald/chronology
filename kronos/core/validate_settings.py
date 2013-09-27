from importlib import import_module
import re

from kronos.core.exceptions import ImproperlyConfigured
from kronos.core.validators import validate_stream
from kronos.core.validators import validate_storage_settings

def get_value(options, options_name, key, type):
  """
  Check that `options` has a value for `key` with type
  `type`. Return that value. `options_name` is a string representing a
  human-readable name for `options` to be used when printing errors.
  """
  if isinstance(options, dict):
    has = lambda k: options.has_key(k)
    get = lambda k: options[k]
  elif isinstance(options, object):
    has = lambda k: hasattr(options, k)
    get = lambda k: getattr(options, k)
  else:
    raise ImproperlyConfigured(
        '`{}` must be a dictionary-like object.'.format(options_name))

  if not has(key):
    raise ImproperlyConfigured(
        '`{}` must be specified in `{}`'.format(key, options_name))

  value = get(key)
  if not isinstance(value, type):
    raise ImproperlyConfigured(
        '`{}` in `{}` must be a {}'.format(key, options_name, repr(type)))

  return value


def validate_settings(settings):
  """
  `settings` is either a dictionary or an object containing Kronos settings
  (e.g., the contents of conf/settings.py). This function checks that all
  required settings are present and valid.
  """

  # Validate `storage`
  storage = get_value(settings, 'settings', 'storage', dict)
  for name, options in storage.iteritems():
    if 'backend' not in options:
      raise ImproperlyConfigured(
          '`storage[\'{}\'] must contain a `backend` key'.format(name))

    path = 'kronos.storage.backends.%s' % options['backend']
    module, cls = path.rsplit('.', 1)
    module = import_module(module)
    if not hasattr(module, cls):
      raise NotImplementedError('`{}` not implemented.'.format(cls))
    validate_storage_settings(getattr(module, cls), options)

  # Validate `streams_to_backends`
  streams_to_backends = get_value(settings, 'settings',
                                  'streams_to_backends', dict)
  if '' not in streams_to_backends:
    raise ImproperlyConfigured(
        'Must specify backends for the null prefix')

  for prefix, options in streams_to_backends.iteritems():
    # Validate stream prefix.
    validate_stream(prefix)

    backends = get_value(options, 'streams_to_backends[\'{}\']'.format(prefix),
                         'backends', dict)
    for backend in backends.keys():
      if backend not in storage:
        raise ImproperlyConfigured(
            '`{}` backend for `streams_to_backends[{}]` is not configured '+
            'in `storage`'.format(backend, prefix))

    read_backend = get_value(options, 'streams_to_backends[{}]'.format(prefix),
                             'read_backend', str)
    if read_backend not in storage:
      raise ImproperlyConfigured(
          '`{}` backend for `streams_to_backends[{}]` is not configured in '+
          '`storage`'.format(read_backend, prefix))

  # Validate `stream`
  stream = getattr(settings, 'stream', dict)
  get_value(stream, 'stream', 'fields', dict)
  get_value(stream, 'stream', 'format', re._pattern_type)

  # Validate `node`
  node = getattr(settings, 'node', dict)
  get_value(node, 'node', 'greenlet_pool_size', int)
  get_value(node, 'node', 'id', str)
