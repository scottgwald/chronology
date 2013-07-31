# The Cassandra storage backend uses (stream_name, time interval) as
# rowkeys and UUIDs as column names. UUIDs serve as precise timestamps, but also
# differentiates two events that occured at the same time. This ensures that
# Kronos will never store more than one event at a given (rowkey, column) pair.
#
#
### Storing an event
#
# Suppose we want to store an event that occured at time `t` in a stream named
# 'clicks'.
#   - The first part of the rowkey is 'clicks'.
#   - From settings.py, we get the default width of a time interval `w`.  We
#     find which of the intervals [0,w), [w,2w), ... that `t` lies in to construct
#     the second part of the rowkey.
# Also from settings.py, we get the sharding factor `s`. Kronos will spread the
# load for a given rowkey across `s` different shards. For each event, Kronos
# will pick one of the `s` shards at random to store the event.
#
# The client should have included a UUID for the event based on the time and
# the client's hardware address which Kronos will use as the column.
#
#
### Retrieving events
#
# Now suppose we want to retrieve all events in a stream named `s` that occured
# in a time interval [a,b). For each time interval `i` of [0,w), [2w,w), ...
# that intersects [a,b) we read the row with key (`s`, `i`) from all shards.
# Reading a row in Cassandra returns all (column, value) pairs in that row, and
# we stream these values back to the caller in UUID order. Note that we may need
# to prune some events out of this stream if they occured in an intersecting
# interval but <a or >=b.
#
#
#
### Indices
#
# Indices help determine which buckets need to be scanned to retrieve events
# between a given start and end time.
# TODO(meelap): Finish documenting indexes.


# TODO(usmanm): Cython some code to speed it?
import heapq
import itertools
import json
import random

from collections import defaultdict
from datetime import timedelta
from pycassa import ColumnFamily, ConnectionPool, NotFoundException, types
from pycassa.batch import Mutator
from pycassa.system_manager import SIMPLE_STRATEGY, SystemManager
from types import StringTypes
from uuid import UUID

from kronos.conf import settings
from kronos.core.exceptions import InvalidBucketIntervalComparison
from kronos.core.exceptions import InvalidUUIDComparison
from kronos.storage.backends import BaseStorage
from kronos.utils.cache import InMemoryLRUCache
from kronos.utils.math import round_down
from kronos.utils.math import time_to_kronos_time
from kronos.utils.math import uuid_from_kronos_time
from kronos.utils.math import uuid_to_kronos_time

ID_FIELD = settings.stream['fields']['id']
TIMESTAMP_FIELD = settings.stream['fields']['timestamp']
LENGTH_OF_YEAR = int(timedelta(days=365.25).total_seconds() * 1e7)


class CassandraSortedUUID(UUID):
  """
  Columns in Cassandra are identified by a UUID.
  Override Python's UUID comparator so that time is the first parameter used for
  sorting.
  """
  def __init__(self, *args, **kwargs):
    super(CassandraSortedUUID, self).__init__(*args, **kwargs)
    self._kronos_time = uuid_to_kronos_time(self)

  def __setattr__(self, name, value):
    # Override UUID's __setattr__ method to make it mutable.
    super(UUID, self).__setattr__(name, value)

  def __cmp__(self, other):
    if isinstance(other, UUID):
      return cmp((self.time, self.bytes), (other.time, other.bytes))
    if other is None:
      return 1
    if isinstance(other, StringTypes):
      return cmp(str(self), other)
    raise InvalidUUIDComparison("Compared CassandraSortedUUID to type {0}"
                                  .format(type(other)))

class BucketInterval(object):
  """
  BucketInterval describes one row in the Cassandra backend. It represents a set
  of events for `stream` that occur in the interval [ `bucket_key[0]`,
  `bucket_key[0]+bucket_key[1]` ) and are stored on shard `shard`.
  """

  def __init__(self, column_family, stream, bucket_key, shard):
    """
    `column_family` : The column family from which to fetch data (events or
                      index).
    `stream` : The name of the stream associated with this BucketInterval.
    `bucket_key` : A tuple of start time and interval length.
    `shard` : The id of the shard that this BucketInterval represents.
    """
    # The name of this BucketInterval as it is stored in Cassandra.
    self.name = BucketInterval.name(stream, bucket_key[0], shard)

    # Starting timestamp of this BucketInterval
    self.start = bucket_key[0]

    # Ending timestamp of this BucketInterval
    self.end = bucket_key[0] + bucket_key[1]

    self.column_family = column_family

  def __cmp__(self, other):
    if other is None:
      return 1
    elif isinstance(other, BucketInterval):
      return cmp((self.start, self.name), (other.start, other.name))

    try:
      return cmp(self.start, float(other))
    except Exception as e:
      raise InvalidBucketIntervalComparison(repr(e))

  def fetch(self):
    """
    Return all events contained in this BucketInterval as a dictionary mapping
    column name (which is a CassandraSortedUUID) to column value.
    """
    try:
      num_cols = self.column_family.get_count(self.name)
      data = self.column_family.get(self.name, column_count=num_cols)
      result = {}
      for column, event in data.iteritems():
        try:
          event = json.loads(event)
          result[CassandraSortedUUID(str(column))] = event
        except:
          # TODO(meelap): Do something about this error. Log?
          pass
      return result
    except NotFoundException:
      # Nothing was stored with this key.
      # This might happen if some events were stored with a sharding factor of
      # `s`, but less than `s` shards were actually used.
      return None

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
  returns them in the order defined by CassandraSortedUUID.
  """
  def __init__(self, intervals, start_id, end_id):
    """
    `intervals` : A list of BucketIntervals to return events from.
    `start_id` : The UUID of the first event to return.
    `end_id` : The UUID of the last event to return. Assumes start_id <= end_id.
    """
    self.start_id = CassandraSortedUUID(str(start_id))
    self.end_id = CassandraSortedUUID(str(end_id))

    self.ready_heap = []
    self.bucket_heap = intervals
    heapq.heapify(self.bucket_heap)

  def load_next_bucket(self):
    if not self.bucket_heap:
      return

    # Pick the next bucket with the earliest start time to load.
    bucket_to_add = heapq.heappop(self.bucket_heap)

    # `events` maps CassandraSortedUUIDs to blobs.
    events = bucket_to_add.fetch()

    if events is not None:
      self.ready_heap.extend(events.items())
      heapq.heapify(self.ready_heap)

  def __iter__(self):
    # Load buckets until we have events to return.
    while not self.ready_heap and self.bucket_heap:
      self.load_next_bucket()

    # The result set is empty.
    if not self.ready_heap:
      raise StopIteration

    # Keep going as long as we have events to return or buckets that we haven't 
    # loaded yet.
    while self.ready_heap or self.bucket_heap:
      if not self.ready_heap:
        # If no events are ready to return but there are buckets left, fetch a
        # bucket
        self.load_next_bucket()
      else:
        # Get the next event to return.
        (uuid, event) = heapq.heappop(self.ready_heap)

        # Yield it if it is in the correct interval, or stop the iteration if we
        # have extended beyond the requested interval.
        if self.end_id < uuid:
          raise StopIteration
        elif self.start_id <= uuid:
          yield event

        # Load more buckets if they start before the time of the next event in
        # the ready_heap.
        if self.ready_heap:
          (next_uuid, next_event) = self.ready_heap[0]
          while self.bucket_heap:
            next_bucket_start = self.bucket_heap[0].start
            if next_bucket_start <= next_uuid._kronos_time:
              self.load_next_bucket()
            else:
              break

class TimeWidthCassandraStorage(BaseStorage):
  # TODO(meelap): Put `read_size` stuff back so that we can limit how much
  # memory Kronos uses to buffer read data from backends.
  EVENT_CF = 'events'
  INDEX_CF = 'index'
  MAX_WIDTH = LENGTH_OF_YEAR / 2 # Too big, small, necessary?

  SETTINGS_VALIDATORS = {
    'default_timewidth_seconds': 
         lambda x: (int(x) > 0 and 
                    time_to_kronos_time(int(x)) <= TimeWidthCassandraStorage.MAX_WIDTH),
    'default_shards_per_bucket': lambda x: int(x) > 0,
    'hosts': lambda x: isinstance(x, list),
    'keyspace': lambda x: len(str(x)) > 0,
    'replication_factor': lambda x: int(x) >= 0,
    'backend': lambda x: x == 'cassandra.TimeWidthCassandraStorage',
  }  

  def __init__(self, name, **settings):
    """
    Check that settings contains all of the required parameters in the right
    format, then setup a connection to the specified Cassandra instance.
    """
    super(TimeWidthCassandraStorage, self).__init__(name, **settings)
    required_params = ('hosts',
                       'keyspace',
                       'replication_factor',
                       'default_timewidth_seconds',
                       'default_shards_per_bucket')
    for param in required_params:
      setattr(self, param, settings[param])

    self.index_cache = InMemoryLRUCache() # 1000-entry LRU cache.
    self.setup_cassandra()
    
  def setup_cassandra(self):
    """
    Set up a connection pool to the specified Cassandra instances and create the
    specified keyspace if it does not exist.
    """
    # TODO(meelap) Don't assume we can connect to the first host. Round robin
    # across hosts until we can connect to one.
    self.system_manager = SystemManager(self.hosts[0])
    if self.keyspace not in self.system_manager.list_keyspaces():
      self.system_manager.create_keyspace(
          self.keyspace,
          SIMPLE_STRATEGY,
          {'replication_factor': str(self.replication_factor)})
      self.system_manager.create_column_family(
          self.keyspace,
          TimeWidthCassandraStorage.EVENT_CF,
          comparator_type=types.TimeUUIDType())
      self.system_manager.create_column_family(
          self.keyspace,
          TimeWidthCassandraStorage.INDEX_CF,
          # Column key: (start_time, width, shard).
          comparator_type=types.CompositeType(types.LongType(),
                                              types.IntegerType(),
                                              types.IntegerType()))
    self.pool = ConnectionPool(keyspace=self.keyspace, server_list=self.hosts)
    self.index_cf = ColumnFamily(self.pool, TimeWidthCassandraStorage.INDEX_CF)
    self.event_cf = ColumnFamily(self.pool, TimeWidthCassandraStorage.EVENT_CF)
    
  def is_alive(self):
    """
    Is our connection to Cassandra alive?
    """
    try:
      connection = self.pool.get()
      connection.describe_keyspace(self.keyspace) # Fake *ping* Cassandra?
      connection.return_to_pool()
      return True
    except:
      return False

  def insert(self, stream, events, configuration):
    """
    Store the specified events for the named stream.
    `stream` : The name of a stream.
    `events` : A list of events to store for the named stream.
    `configuration` : A dictionary of settings to override any default settings,
                      such as number of shards or width of a time interval.
    """
    width = configuration.get('timewidth_seconds', self.default_timewidth_seconds)
    width = time_to_kronos_time(width)
    shards = int(configuration.get('shards_per_bucket', self.default_shards_per_bucket))
    shard = random.randint(0, shards - 1)

    index_to_buckets = defaultdict(dict) # (stream, index_start) => [(stream, bucket_start, shard)]
    bucket_to_events = defaultdict(dict) # (stream, bucket_start, shard) => {id => properties}

    # Group together all events that are in the same bucket so that
    # bucket_to_events maps bucketnames to { column_name==UUID : event, ... }.
    for event in events:
      bucket_start_time = round_down(event[TIMESTAMP_FIELD], width)
      bucket_name = BucketInterval.name(stream, bucket_start_time, shard)
      bucket_to_events[bucket_name][UUID(event[ID_FIELD])] = json.dumps(event)
      index_start_time = round_down(event[TIMESTAMP_FIELD], LENGTH_OF_YEAR)
      index = '%s:%s' % (stream, index_start_time)
      index_to_buckets[index][(bucket_start_time, width, shards)] = ''

    mutator = Mutator(self.pool, queue_size=1000)

    # Add all event writes to the batch of operations.
    for bucket_name, events in bucket_to_events.iteritems():
      mutator.insert(self.event_cf, bucket_name, events)

    # Add all index writes to the batch of operations.
    for index, buckets in index_to_buckets.iteritems():
      try:
        cached_index_value = self.index_cache.get(index)
        new_index_value = set(buckets) | cached_index_value
        if new_index_value != cached_index_value:
          # Write the new buckets covered by this index entry.
          mutator.insert(self.index_cf, index,
                         dict.fromkeys(new_index_value - cached_index_value, ''))
          self.index_cache.set(index, new_index_value)
      except KeyError:
        mutator.insert(self.index_cf, index, buckets)
        self.index_cache.set(index, set(buckets))

    # Send the current batch of operations to Cassandra.
    mutator.send()
    
  def _retrieve(self, stream, start_id, end_time, configuration):
    """
    Retrieve events for `stream` between `start_id` and `end_time`.
    `stream` : The stream to return events for.
    `start_id` : Return events with id > `start_id`.
    `end_time` : Return events ending <= `end_time`.
    `configuration` : A dictionary of settings to override any default settings,
                      such as number of shards or width of a time interval.
    """
    # Time of the first event to return
    start_time = uuid_to_kronos_time(start_id)

    # Time of the oldest bucket that could possibly contain the first event.
    bucket_start_time = max(start_time - TimeWidthCassandraStorage.MAX_WIDTH, 0)

    # Smallest possible width of the oldest bucket that could possibly contain
    # the first event.
    bucket_start_width = start_time - bucket_start_time

    # Time of the last event to return.
    end_time = long(end_time)

    # Index width is one year. Get all indices pointing to buckets which
    # intersect with our time interval of interest.
    indexes_to_scan = ['%s:%s' % (stream, i) for i in
                       range(round_down(bucket_start_time, LENGTH_OF_YEAR),
                             round_down(end_time, LENGTH_OF_YEAR) +
                             LENGTH_OF_YEAR,
                             LENGTH_OF_YEAR)]

    # Get all buckets which might contain events of interest.
    index_keys = self.index_cf.multiget(indexes_to_scan,
                                        column_start=(bucket_start_time,
                                                      bucket_start_width, 0),
                                        column_finish=(end_time, 0, 0),
                                        buffer_size=len(indexes_to_scan))

    # Construct a list of BucketIntervals to scan for matching events.
    intervals = []
    for bucket_key in itertools.chain.from_iterable(index_keys.itervalues()):
      for i in xrange(bucket_key[2]):
        intervals.append(BucketInterval(self.event_cf, stream, bucket_key, i))

    end_id = uuid_from_kronos_time(end_time, lowest=False)
    events = SortedShardedEventStream(intervals, start_id, end_id)
    for event in events:
      yield event

  def streams(self):
    # TODO(usmanm): Ideally, we don't wanna keep an in-memory set of all stream 
    # names because it could cause memory issues. How to dedup?
    streams = set()
    for index in self.index_cf.get_range(column_count=0, filter_empty=False):
      stream = index[0].split(':')[0]
      if stream not in streams:
        streams.add(stream)
        yield stream
