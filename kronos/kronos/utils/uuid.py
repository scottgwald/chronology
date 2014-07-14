from __future__ import absolute_import

from uuid import UUID
from uuid import uuid4
from timeuuid import TimeUUID

UUID_TIME_OFFSET = 0x01b21dd213814000L


class UUIDType(object):
  LOWEST = 'lowest'
  HIGHEST = 'highest'
  RANDOM = 'random'

def uuid_to_kronos_time(uuid):
  """
  UUIDs contain a time field. Convert it to kronos time and return.
  """

  if not isinstance(uuid, (TimeUUID, UUID)):
    raise Exception('Expected type TimeUUID/UUID')
  return uuid.time - 0x01b21dd213814000L


def uuid_from_kronos_time(time, _type=UUIDType.RANDOM):
  """
  Generate a UUID with the specified time.
  If `lowest` is true, return the lexicographically first UUID for the specified
  time.
  """
  # Bit-flipping logic from uuid1 implementation described in:
  # http://stackoverflow.com/questions/7153844/uuid1-from-utc-timestamp-in-python
  # except we use a random UUID to seed the clock sequence to minimize the
  # probability of two calls to this function with the same time getting the
  # same ID.
  timestamp = int(time) + UUID_TIME_OFFSET
  time_low = timestamp & 0xffffffffL
  time_mid = (timestamp >> 32L) & 0xffffL
  time_hi_version = (timestamp >> 48L) & 0x0fffL
  if _type == UUIDType.LOWEST:
    clock_seq_low = 0 & 0xffL
    clock_seq_hi_variant = 0 & 0x3fL
    node = 0 & 0xffffffffffffL
  elif _type == UUIDType.HIGHEST:
    clock_seq_low = 0xffL
    clock_seq_hi_variant = 0x3fL
    node = 0xffffffffffffL
  else:
    randomuuid = uuid4()
    clock_seq_low = randomuuid.clock_seq_low
    clock_seq_hi_variant = randomuuid.clock_seq_hi_variant
    node = randomuuid.node
  uuid = UUID(fields=(time_low,
                      time_mid,
                      time_hi_version,
                      clock_seq_hi_variant,
                      clock_seq_low,
                      node),
              version=1)
  return TimeUUID(str(uuid))


LOWEST_UUID = uuid_from_kronos_time(0 - UUID_TIME_OFFSET, UUIDType.LOWEST)
HIGHEST_UUID = uuid_from_kronos_time(2**60 - 1 - UUID_TIME_OFFSET,
                                     UUIDType.HIGHEST)
