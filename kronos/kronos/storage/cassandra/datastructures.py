import heapq
import json

from pycassa import NotFoundException
from types import StringTypes
from uuid import UUID

from kronos.conf.constants import ResultOrder
from kronos.conf.constants import ID_FIELD
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
    raise Exception('Compared CassandraUUID to type {0}'
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
    raise Exception('Compared CassandraEvent to type {0}'
                    .format(type(other)))


class BucketInterval(object):
  """
  BucketInterval describes one row in the Cassandra backend. It represents a set
  of events for `stream` that occur in the interval [ `bucket_key[0]`,
  `bucket_key[0]+bucket_key[1]` ) and are stored on shard `shard`.
  """

  def __init__(self, column_family, stream, bucket_key, shard, order,
               read_size):
    """
    `column_family` : The column family from which to fetch data (events or
                      index).
    `stream` : The name of the stream associated with this BucketInterval.
    `bucket_key` : A tuple of start time and interval length.
    `shard` : The id of the shard that this BucketInterval represents.
    `order` : Whether to return the results in ResultOrder.ASCENDING
              or ResultOrder.DESCENDING time-order.
    `read_size` : The number of events to fetch from Cassandra per chunk.
                  Optimization to reduce memory usage.
    """
    # The name of this BucketInterval as it is stored in Cassandra.
    self.name = BucketInterval.name(stream, bucket_key[0], shard)
    # Starting timestamp of this BucketInterval
    self.start = bucket_key[0]
    # Ending timestamp of this BucketInterval
    self.end = bucket_key[0] + bucket_key[1]
    self.column_family = column_family
    self.order = order
    self.read_size = read_size

    # If we want to sort in descending order, compare the end of the
    # interval.
    self._cmp_value = (self.start if order == ResultOrder.ASCENDING
                       else -self.end)

  def __cmp__(self, other):
    if other is None:
      return 1
    elif isinstance(other, BucketInterval):
      return cmp((self._cmp_value, self.name), (other._cmp_value, other.name))

    try:
      return cmp(self._cmp_value, float(other))
    except Exception as e:
      raise InvalidBucketIntervalComparison(repr(e))

  def iterator(self, column_start, column_finish):
    """
    Return an iterator over all CassandraEvents contained in this BucketInterval
      `column_start`, `column_finish`: For optimization purposes, send the
        maximum values for column IDs so that Cassandra can filter them
        before sending back a large bucket.
    """
    try:
      num_cols = self.column_family.get_count(self.name,
                                              column_start=column_start,
                                              column_finish=column_finish)
      if num_cols == 0:
        raise NotFoundException
      # If read_size is <= 0, fetch all columns in one go.
      if self.read_size <= 0:
        buffer_size = num_cols
      else:
        buffer_size = self.read_size
      column_reversed = self.order == ResultOrder.DESCENDING
      if column_reversed:
        column_start, column_finish = column_finish, column_start
      data = self.column_family.xget(self.name,
                                     column_start=column_start,
                                     column_finish=column_finish,
                                     column_count=num_cols,
                                     column_reversed=column_reversed,
                                     buffer_size=buffer_size)
      for column, event in data:
        try:
          yield CassandraEvent(event, self, order=self.order)
        except GeneratorExit:
          raise StopIteration
        except ValueError:
          pass
    except NotFoundException:
      # Nothing was stored with this key.
      # This might happen if some events were stored with a sharding factor of
      # `s`, but less than `s` shards were actually used.
      raise StopIteration

  @staticmethod
  def name(stream, start_time, shard):
    """
    Return the string used as the rowkey for the BucketInterval with the
    specified attributes.
    """
    return '{0}:{1}:{2}'.format(stream, start_time, shard)


class SortedShardedEventStream(object):
  """
  SortedShardedEventStream is an iterable that fetches events from Cassandra and
  returns them in the order defined by CassandraUUID.
  """
  def __init__(self, intervals, start_id, end_id, limit, order):
    """
    `intervals` : A list of BucketIntervals to return events from.
    `start_id` : The UUID of the first event to return.
    `end_id` : The UUID of the last event to return. Assumes start_id <= end_id.
    `order` : Whether to return the results in ResultOrder.ASCENDING
              or ResultOrder.DESCENDING time-order.    
    """
    self.order = order
    self.limit = limit
    self.start_id = CassandraUUID(str(start_id), order=self.order)
    self.end_id = CassandraUUID(str(end_id), order=self.order)

    self.iterators = {}
    self.merge_heap = []
    self.bucket_heap = intervals
    heapq.heapify(self.bucket_heap)

  def load_events(self):
    """
    Given what the current most recently loaded event is, loads any
    buckets that might overlap with that event.  Multiple buckets
    might overlap because they have overlapping time slices or shards.
    """
    while (not self.merge_heap) and self.bucket_heap:
      self.load_next_bucket()
    if self.merge_heap:
      next_event = self.merge_heap[0]
      while self.bucket_heap:
        next_bucket_time = (self.bucket_heap[0].start
                            if self.order == ResultOrder.ASCENDING
                            else self.bucket_heap[0].end)
        if ((self.order == ResultOrder.ASCENDING and
             next_bucket_time > next_event.id._kronos_time) or
            (self.order == ResultOrder.DESCENDING and
             next_bucket_time < next_event.id._kronos_time)):
          break
        self.load_next_bucket()

    for bucket in (set(self.iterators) -
                   set(event.bucket for event in self.merge_heap)):
      try:
        iterator = self.iterators[bucket]
        event = iterator.next()
        heapq.heappush(self.merge_heap, event)        
      except StopIteration:
        del self.iterators[bucket]
    
  def load_next_bucket(self):
    if not self.bucket_heap:
      return

    # Pick the next bucket with the earliest start time to load.
    bucket_to_add = heapq.heappop(self.bucket_heap)

    # `iterator` is an iterator over events in the bucket.
    iterator = bucket_to_add.iterator(self.start_id, self.end_id)
    try:
      event = iterator.next()
      heapq.heappush(self.merge_heap, event)
      self.iterators[bucket_to_add] = iterator
    except StopIteration:
      pass

  def __iter__(self):
    # Load buckets until we have events to return.
    while (not self.merge_heap) and self.bucket_heap:
      self.load_events()

    # The result set is empty.
    if not self.merge_heap:
      raise StopIteration

    # Keep going as long as we have events to return or buckets that we haven't 
    # loaded yet.
    while self.merge_heap or self.bucket_heap:
      if self.limit <= 0:
        raise StopIteration
      if self.merge_heap:
        # Get the next event to return.
        event = heapq.heappop(self.merge_heap)
        
        # Yield it if it is in the correct interval, or stop the iteration if we
        # have extended beyond the requested interval.
        # Note: in ResultOrder.DESCENDING conditions below, we flip `<` for `>`
        # and `>=` for `<=` UUID comparator logic is flipped.
        # TODO(marcua): convince myself that the edge cases on DESCENDING
        # have equality in the correct place for start_id and end_id.
        if ((self.order == ResultOrder.ASCENDING and
             self.end_id < event) or
            (self.order == ResultOrder.DESCENDING and
             self.start_id < event)):
          raise StopIteration
        elif ((self.order == ResultOrder.ASCENDING and
               self.start_id <= event) or
              (self.order == ResultOrder.DESCENDING and
               self.end_id <= event)):
          self.limit -= 1
          yield event.raw_event

      self.load_events()
