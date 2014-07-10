from __future__ import absolute_import

from uuid import UUID
from uuid import uuid4

from kronos.conf.constants import ResultOrder
from kronos.core.exceptions import InvalidTimeUUIDComparison

UUID_TIME_OFFSET = 0x01b21dd213814000L


class UUIDType(object):
  LOWEST = 'lowest'
  HIGHEST = 'highest'
  RANDOM = 'random'


class TimeUUID(UUID):
  """
  Override Python's UUID comparator so that time is the first parameter used
  for sorting. This is the comparator behavior for `timeuuid` types in
  Cassandra.
  """
  def __init__(self, *args, **kwargs):
    """
    `order`[kwarg]: Whether to return the results in
             ResultOrder.ASCENDING or ResultOrder.DESCENDING
             time-order.
    """
    # TODO(marcua): Couldn't get `order` to be a named arg (because of
    # subclassing?).  I don't like the next line.
    if args and isinstance(args[0], UUID):
      args = list(args)
      args[0] = str(args[0])
    order = kwargs.pop('order', ResultOrder.ASCENDING)
    kwargs['version'] = 1
    super(TimeUUID, self).__init__(*args, **kwargs)

    self._kronos_time = uuid_to_kronos_time(self)
    self._cached_bytes = self.bytes
    self._cmp_multiplier = ResultOrder.get_multiplier(order)
    
  def __setattr__(self, name, value):
    # Override UUID's __setattr__ method to make it mutable.
    super(UUID, self).__setattr__(name, value)

  def __cmp__(self, other):
    if other is None:
      return 1
    if isinstance(other, TimeUUID):
      return self._cmp_multiplier * cmp((self._kronos_time,
                                         self._cached_bytes),
                                        (other._kronos_time,
                                         other._cached_bytes))
    raise InvalidTimeUUIDComparison('Compared TimeUUID to type {0}'
                                    .format(type(other)))


def uuid_to_kronos_time(uuid):
  """
  UUIDs contain a time field. Convert it to kronos time and return.
  """

  if not isinstance(uuid, UUID):
    raise Exception('Expected type UUID')
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
  uuid = TimeUUID(fields=(time_low,
                          time_mid,
                          time_hi_version,
                          clock_seq_hi_variant,
                          clock_seq_low,
                          node),
                  version=1)
  return uuid


LOWEST_UUID = uuid_from_kronos_time(0 - UUID_TIME_OFFSET, UUIDType.LOWEST)
HIGHEST_UUID = uuid_from_kronos_time(2**60 - 1 - UUID_TIME_OFFSET,
                                     UUIDType.HIGHEST)
