import re
import time

from uuid import uuid4, UUID

from kronos.conf import settings
from kronos.core.exceptions import (InvalidEventId,
                                    InvalidEventTime,
                                    InvalidStreamName)

ID_FIELD = settings.stream['fields']['id']
TIMESTAMP_FIELD = settings.stream['fields']['timestamp']
STREAM_REGEX = re.compile(r'^[a-z0-9\_]+(\.[a-z0-9\_]+)*$', re.I)

def validate_event(event):
  event_time = event.get(TIMESTAMP_FIELD)
  event_id = event.get(ID_FIELD)

  # Is event timestamp valid?
  if event_time is None:
    if event_id is None:
      event[TIMESTAMP_FIELD] = time.time()
  elif type(event_time) not in (int, float):
    raise InvalidEventTime(event_time)

  # Is event ID valid?
  if not event_id is None:
    try:
      uuid = UUID(event_id)
      if event_time is None:
        event[TIMESTAMP_FIELD] = (uuid.time - 0x01b21dd213814000L) * 100 / 1e9
    except ValueError:
      raise InvalidEventId(event_id)
  else:
    # If no ID is provided, generate a uuid1-like sequence from the current
    # time. This isn't totally kosher: the UUID1 node comes from the MAC address
    # of this machine rather than that of the client, but we're using uuids as
    # a proxy for unique pseudotime-ordered strings anyway, so the damage has
    # already been done :).
    #
    # Bit-flipping logic from uuid1 implementation described in:
    # http://stackoverflow.com/questions/7153844/uuid1-from-utc-timestamp-in-python
    # except we use a random UUID to seed the clock sequence to minimize the
    # probability of two events with the same timestamp getting the same ID.
    nanoseconds = int(event[TIMESTAMP_FIELD] * 1e9)
    timestamp = int(nanoseconds/100) + 0x01b21dd213814000L
    time_low = timestamp & 0xffffffffL
    time_mid = (timestamp >> 32L) & 0xffffL
    time_hi_version = (timestamp >> 48L) & 0x0fffL
    randomuuid = uuid4()
    uuid = UUID(fields=(time_low, time_mid, time_hi_version,
                        randomuuid.clock_seq_hi_variant,
                        randomuuid.clock_seq_low, randomuuid.node),
                version=1)
    event[ID_FIELD] = str(uuid)

  verified_timestamp = (uuid.time - 0x01b21dd213814000L) * 100 / 1e9
  if abs(verified_timestamp - event[TIMESTAMP_FIELD]) > 0.01:
    raise InvalidEventTime('Mismatch timestamp (%f vs. %f) for event id [%s].' %
                           (verified_timestamp, event[TIMESTAMP_FIELD], uuid))

def validate_stream(stream):
  if not STREAM_REGEX.match(stream):
    raise InvalidStreamName(stream)
