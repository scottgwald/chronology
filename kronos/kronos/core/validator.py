import re
import time

from importlib import import_module

from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.core.errors import ImproperlyConfigured
from kronos.core.errors import InvalidEventTime
from kronos.core.errors import InvalidStreamName
from kronos.utils.math import time_to_kronos_time
from kronos.utils.uuid import uuid_from_kronos_time

MAX_STREAM_LENGTH = 2048
STREAM_REGEX = re.compile(r'^[a-z0-9\_]+(\.[a-z0-9\_]+)*$', re.I)


def _validate_and_get_value(options, options_name, key, _type):
  """
  Check that `options` has a value for `key` with type
  `_type`. Return that value. `options_name` is a string representing a
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
  if not isinstance(value, _type):
    raise ImproperlyConfigured(
        '`{}` in `{}` must be a {}'.format(key, options_name, repr(_type)))

  return value


def validate_event_and_assign_id(event):
  """
  Ensure that the event has a valid time. Assign a random UUID based on the
  event time.
  """
  event_time = event.get(TIMESTAMP_FIELD)

  if event_time is None:
    event[TIMESTAMP_FIELD] = event_time = time_to_kronos_time(time.time())
  elif type(event_time) not in (int, long):
    raise InvalidEventTime(event_time)

  # Generate a uuid1-like sequence from the event time with the non-time bytes
  # set to random values.
  _id = uuid_from_kronos_time(event_time)
  event[ID_FIELD] = str(_id)
  return _id, event


def validate_stream(stream):
  """
  Check that the stream name is well-formed.
  """
  if not STREAM_REGEX.match(stream) or len(stream) > MAX_STREAM_LENGTH:
    raise InvalidStreamName(stream)


def validate_storage_settings(storage_class, settings):
  """
  Given a `storage_class` and a dictionary of `settings` to initialize it,
  this method verifies that all the settings are valid.
  """
  if not isinstance(settings, dict):
    raise ImproperlyConfigured(
        '{}: storage class settings must be a dict'.format(storage_class))

  if not hasattr(storage_class, 'SETTINGS_VALIDATORS'):
    raise NotImplementedError(
        '{}: storage class must define `SETTINGS_VALIDATORS`'.format(storage_class))

  settings_validators = getattr(storage_class, 'SETTINGS_VALIDATORS')
  settings = settings.copy()
  settings.pop('backend', None) # No need to validate the `backend` key.
  invalid_settings = set(settings.keys()) - set(settings_validators.keys())
  if invalid_settings:
    raise ImproperlyConfigured(
        '{}: invalid settings: {}'.format(storage_class, invalid_settings))

  for setting, value in settings.iteritems():
    if not settings_validators[setting](value):
      raise ImproperlyConfigured(
          '{}: invalid value for {}'.format(storage_class, setting))


def validate_settings(settings):
  """
  `settings` is either a dictionary or an object containing Kronos settings
  (e.g., the contents of conf/settings.py). This function checks that all
  required settings are present and valid.
  """

  # Validate `storage`
  storage = _validate_and_get_value(settings, 'settings', 'storage', dict)
  for name, options in storage.iteritems():
    if 'backend' not in options:
      raise ImproperlyConfigured(
          '`storage[\'{}\'] must contain a `backend` key'.format(name))

    path = 'kronos.storage.%s' % options['backend']
    module, cls = path.rsplit('.', 1)
    module = import_module(module)
    if not hasattr(module, cls):
      raise NotImplementedError('`{}` not implemented.'.format(cls))
    validate_storage_settings(getattr(module, cls), options)

  # Validate `streams_to_backends`
  namespace_to_streams_configuration = _validate_and_get_value(
      settings, 'settings', 'namespace_to_streams_configuration', dict)
  for namespace, prefix_confs in namespace_to_streams_configuration.iteritems():
    if '' not in prefix_confs:
      raise ImproperlyConfigured(
          'Must specify backends for the null prefix')

    for prefix, options in prefix_confs.iteritems():
      if prefix != '':
        # Validate stream prefix.
        validate_stream(prefix)

      backends = _validate_and_get_value(
          options, 
          "namespace_to_streams_configuration['{}']['{}']".format(namespace, 
                                                                  prefix),
          'backends', dict)
      for backend in backends.keys():
        if backend not in storage:
          raise ImproperlyConfigured(
              "`{}` backend for `namespace_to_streams_configuration['{}']"
              "['{}']` is not configured in `storage`"
              .format(backend, namespace, prefix))

      read_backend = _validate_and_get_value(
          options,
          "namespace_to_streams_configuration['{}']['{}']".format(namespace, 
                                                                  prefix),
          'read_backend', str)
      if read_backend not in storage:
          raise ImproperlyConfigured(
              "`{}` backend for `namespace_to_streams_configuration['{}']"
              "['{}']` is not configured in `storage`"
              .format(read_backend, namespace, prefix))

  # Validate `stream`
  stream = getattr(settings, 'stream', dict)
  _validate_and_get_value(stream, 'stream', 'format', re._pattern_type)

  # Validate `node`
  node = getattr(settings, 'node', dict)
  _validate_and_get_value(node, 'node', 'greenlet_pool_size', int)
  _validate_and_get_value(node, 'node', 'id', str)
