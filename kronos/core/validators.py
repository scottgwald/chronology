import re
import time

from uuid import UUID

from kronos.conf import settings

from kronos.core.exceptions import InvalidEventId
from kronos.core.exceptions import InvalidEventTime
from kronos.core.exceptions import InvalidStreamName
from kronos.utils.math import time_to_kronos_time
from kronos.utils.math import uuid_to_kronos_time
from kronos.utils.math import uuid_from_kronos_time

TIMESTAMP_FIELD = settings.stream['fields']['timestamp']
ID_FIELD = settings.stream['fields']['id']
STREAM_REGEX = re.compile(r'^[a-z0-9\_]+(\.[a-z0-9\_]+)*$', re.I)

def validate_event(event):
  """
  Ensure that the event has a valid time and id.
  """

  event_time = event.get(TIMESTAMP_FIELD)
  event_id = event.get(ID_FIELD)

  # If no event time is specified, extract it from the event id if the id
  # exists, otherwise set the time to now.
  if event_time is None:
    if event_id is None:
      event_time = time_to_kronos_time(time.time())
    else:
      try:
        uuid = UUID(event_id)
      except ValueError:
        raise InvalidEventId(event_id)
      event_time = uuid_to_kronos_time(uuid)
  elif type(event_time) not in (int, long, float):
    raise InvalidEventTime(event_time)

  if event_id is None:
    # If no ID is provided, generate a uuid1-like sequence from the event
    # time. This isn't totally kosher: the UUID1 node comes from the MAC address
    # of this machine rather than that of the client, but we're using uuids as
    # a proxy for unique pseudotime-ordered strings anyway, so the damage has
    # already been done :).
    uuid = uuid_from_kronos_time(event_time)

    # Make sure that the time in our uuid is the event's actual time
    if uuid_to_kronos_time(uuid) != event_time:
      raise InvalidEventTime('{0}: mismatch with event id [{1}].'
                              .format(event[TIMESTAMP_FIELD], uuid))

    event_id = str(uuid)
  
  event[TIMESTAMP_FIELD] = event_time
  event[ID_FIELD] = event_id


def validate_stream(stream):
  """
  Check that the stream name is well-formed.
  """

  if not STREAM_REGEX.match(stream):
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
  invalid_settings = set(settings.keys()) - set(settings_validators.keys())
  if invalid_settings:
    raise ImproperlyConfigured(
        '{}: invalid settings: {}'.format(storage_class, invalid_settings))

  for setting, value in settings.iteritems():
    if not settings_validators[setting](value):
      raise ImproperlyConfigured(
          '{}: invalid value for {}'.format(storage_class, setting))

