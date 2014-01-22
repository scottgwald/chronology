import json

from types import StringTypes
from uuid import UUID

from kronos.conf.constants import ResultOrder
from kronos.conf.constants import ID_FIELD
from kronos.core.exceptions import InvalidUUIDComparison
from kronos.utils.math import uuid_to_kronos_time


class CassandraUUID(UUID):
  """
  Columns in Cassandra are identified by a UUID.
  Override Python's UUID comparator so that time is the first parameter used
  for sorting.
  """
  def __init__(self, *args, **kwargs):
    """
    `order`[kwarg]: Whether to return the results in
             ResultOrder.ASCENDING or ResultOrder.DESCENDING
             time-order.
    """
    # TODO(marcua): Couldn't get `order` to be a named arg (because of
    # subclassing?).  I don't like the next line.
    order = kwargs.pop('order', ResultOrder.ASCENDING)
    super(CassandraUUID, self).__init__(*args, **kwargs)
    self._kronos_time = uuid_to_kronos_time(self)

    # If we want to sort in descending order, we'll multiply our
    # comparisons by -1 to reverse them.
    self._cmp_multiplier = 1 if order == ResultOrder.ASCENDING else -1
    
  def __setattr__(self, name, value):
    # Override UUID's __setattr__ method to make it mutable.
    super(UUID, self).__setattr__(name, value)

  def __cmp__(self, other):
    if isinstance(other, StringTypes):
      try:
        other = UUID(other)
      except (ValueError, AttributeError):
        return 1
    if isinstance(other, CassandraEvent):
      other = other.id
    if isinstance(other, UUID):
      return self._cmp_multiplier * cmp((self.time, self.bytes),
                                        (other.time, other.bytes))
    if other is None:
      return 1
    raise InvalidUUIDComparison('Compared CassandraUUID to type {0}'
                                .format(type(other)))


class CassandraEvent(object):
  def __init__(self, event, bucket, order=ResultOrder.ASCENDING):
    if isinstance(event, StringTypes):
      event = json.loads(event)
    self.raw_event = event
    self.id = CassandraUUID(event[ID_FIELD], order=order)
    self.bucket = bucket
  
  def __cmp__(self, other):
    if not other:
      return 1
    if (isinstance(other, UUID) or
        isinstance(other, StringTypes)):
      return cmp(self.id, other)
    if isinstance(other, CassandraEvent):
        return cmp(self.id, other.id)
    raise InvalidUUIDComparison('Compared CassandraEvent to type {0}'
                                .format(type(other)))
