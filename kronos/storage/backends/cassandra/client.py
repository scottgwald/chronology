# TODO(usmanm): Cython some code to speed it?
import cjson
import heapq
import itertools
import math
import random

from blist import sortedset
from collections import defaultdict
from pycassa import ColumnFamily, ConnectionPool, types
from pycassa.batch import Mutator
from pycassa.system_manager import SIMPLE_STRATEGY, SystemManager
from pycassa.util import convert_time_to_uuid
from types import StringTypes
from uuid import UUID

from kronos.core.exceptions import ImproperlyConfigured
from kronos.core.validators import ID_FIELD, TIMESTAMP_FIELD
from kronos.storage.backends import BaseStorage
from kronos.utils.cache import InMemoryLRUCache

SECONDS_IN_YEAR = int(365.25*24*60*60) # Fixed time width for index buckets.

class CassandraSortedUUID(UUID):
  def __init__(self, *args, **kwargs):
    super(CassandraSortedUUID, self).__init__(*args, **kwargs)
    self._time = self.time
    self._time_seconds = (self._time - 0x01b21dd213814000L) * 100 / 1e9
    self._bytes = self.bytes

  def __setattr__(self, name, value):
    super(UUID, self).__setattr__(name, value)
    
  def __cmp__(self, other):
    if other is None:
      return 1
    if isinstance(other, StringTypes):
      return cmp(str(self), other)
    return cmp((self._time, self._bytes), (other._time, other._bytes))

class BucketInterval(object):
  def __init__(self, stream, bucket_key, shard):
    self.name = '%s:%s:%s' % (stream, bucket_key[0], shard)
    self.start = bucket_key[0]
    self.end = bucket_key[0] + bucket_key[1]
    self.count = 0

  def __cmp__(self, other):
    if other is None:
      return 1
    try:
      float(other)
      return cmp(self.start, other)
    except (TypeError, ValueError):
      return cmp((self.start, self.name), (other.start, other.name))
  
class SortedShardedEventStream(object):
  def __init__(self, column_family, intervals, start_id, end_id, read_size):
    self.column_family = column_family
    self.start_id = CassandraSortedUUID(str(start_id))
    self.end_id = CassandraSortedUUID(str(end_id))
    self.read_size = read_size
    bucket_to_count = self.column_family.multiget_count(
        (interval.name for interval in intervals),
        column_start=self.start_id,
        column_finish=self.end_id,
        buffer_size=len(intervals))
    self.bucket_to_interval = dict()
    self.sorted_intervals = sortedset()
    self.end_time_to_intervals = defaultdict(list)
    for interval in intervals:
      interval.count = bucket_to_count[interval.name]
      if not interval.count:
        continue # Skip empty buckets.
      self.bucket_to_interval[interval.name] = interval
      self.sorted_intervals.add(interval)
      self.end_time_to_intervals[interval.end].append(interval)
    self.interval_to_events = defaultdict(list)
    self.exhausted_intervals = set()
    self.interval_to_latest_id = dict()
    self.current_dying_intervals = set()
    self.current_intervals = set()
    self.min_threshold = None

  def fetch_events(self, first=False):
    # We just depleted the earliest ending bucket, so need to fetch buckets
    # which overlap with the new earliest ending bucket. As an optimization,
    # we also refill buckets which are running out of events.
    if not self.current_intervals:
      # No other buckets are in flight? Either first run, or all the buckets
      # we fetched have depleted.
      start_index = self.sorted_intervals.bisect_left(
          0 if first else
          self.start_id._time_seconds)
      num_events = 0
      earliest_end_time = float('inf')
      # Get intervals starting from last interval that was expunged.
      for i in xrange(start_index, len(self.sorted_intervals)):
        interval = self.sorted_intervals[i]
        if interval not in self.exhausted_intervals:
          num_events += max(interval.count, 0)
        earliest_end_time = min(earliest_end_time, interval.end)
        self.current_intervals.add(interval)
        if num_events >= self.read_size:
          break
      # Also get all buckets that start before the earliest ending bucket ends.
      end_index = self.sorted_intervals.bisect_right(earliest_end_time)
      for i in xrange(start_index, end_index):
        interval = self.sorted_intervals[i]
        earliest_end_time = min(earliest_end_time, interval.end)
        self.current_intervals.add(interval)
      self.current_dying_intervals = set(
          self.end_time_to_intervals[earliest_end_time])
      intervals_to_fetch = self.current_intervals
    else: # Cached events for some interval ran out?
      if not self.current_dying_intervals:
        # The earliest end time has changed, figure out the new one.
        earliest_end_time = float('inf')
        for interval in self.current_intervals:
          earliest_end_time = min(earliest_end_time, interval.end)
        # Add intervals which start before new earliest end time.
        start_index = self.sorted_intervals.bisect_left(
          self.start_id._time_seconds)
        end_index = self.sorted_intervals.bisect_right(earliest_end_time)
        for i in xrange(start_index, end_index):
          interval = self.sorted_intervals[i]
          earliest_end_time = min(earliest_end_time, interval.end)
          self.current_intervals.add(interval)
        self.current_dying_intervals = set(
            self.end_time_to_intervals[earliest_end_time])        
      num_events = 0
      earliest_ending_interval = self.current_dying_intervals.pop()
      self.current_dying_intervals.add(earliest_ending_interval)
      intervals_to_fetch = set()
      for interval in itertools.chain(
          self.current_intervals,
          (self.sorted_intervals[i] for i in xrange(
              self.sorted_intervals.bisect_right(earliest_ending_interval.end),
              len(self.sorted_intervals)))):
        if len(self.interval_to_events[interval]) > self.min_threshold:
          continue
        if interval not in self.exhausted_intervals:
          num_events += max(interval.count, 0)
        intervals_to_fetch.add(interval)
        self.current_intervals.add(interval)
        if num_events >= self.read_size:
          break
    intervals_to_fetch -= self.exhausted_intervals
    if not intervals_to_fetch:
      return      
    # Fetch events from Cassandra and cache them.
    events_to_fetch = max(self.read_size / len(self.current_intervals), 2)
    self.min_threshold = math.ceil(0.25 * events_to_fetch)
    bucket_to_events = self.column_family.multiget(
        (interval.name for interval in intervals_to_fetch),
        column_start=self.start_id,
        column_finish=self.end_id,
        column_count=events_to_fetch,
        buffer_size=len(self.current_intervals))
    # Buckets for which no events were returned? Add them to the exhausted set.
    self.exhausted_intervals |= (intervals_to_fetch -
                                 {self.bucket_to_interval[bucket] for
                                  bucket in bucket_to_events})
    for bucket, events in bucket_to_events.iteritems():
      interval = self.bucket_to_interval[bucket]
      if len(events) < events_to_fetch:
        self.exhausted_intervals.add(interval)
      try:
        latest_cached_event_id = self.interval_to_events[interval][-1][ID_FIELD]
      except IndexError:
        latest_cached_event_id = None      
      for event_id, event in events.iteritems():
        event = cjson.decode(event)
        event_id = CassandraSortedUUID(str(event_id))
        if not first and event_id == self.start_id:
          continue
        # Filter dups.
        if (event_id > self.interval_to_latest_id.get(interval) and
            event_id > latest_cached_event_id):
          self.interval_to_events[interval].append(event)
    self.current_intervals -= self.exhausted_intervals
    return (self.bucket_to_interval[bucket] for bucket in bucket_to_events)

  def __iter__(self):
    heap = []
    intervals_in_heap = set()
    # Fetch first batch of events for each bucket and insert smallest event
    # for each bucket in heap.
    self.fetch_events(first=True)
    for interval, events in self.interval_to_events.iteritems():
      event = events.pop(0)
      self.interval_to_latest_id[interval] = event[ID_FIELD]
      interval.count -= 1
      heap.append((CassandraSortedUUID(event[ID_FIELD]), (interval, event)))
      intervals_in_heap.add(interval)
    heapq.heapify(heap)
    while heap:
      event_id, (interval, event) = heap[0]
      event[ID_FIELD] = str(event[ID_FIELD])
      yield event
      self.start_id = event_id
      events_left_for_interval = self.interval_to_events[interval]
      # If the interval has more events remaining, pop the smallest one and
      # insert in heap.
      if events_left_for_interval:
        event = events_left_for_interval.pop(0)
        heapq.heapreplace(heap, (CassandraSortedUUID(event[ID_FIELD]),
                                 (interval, event)))
        self.interval_to_latest_id[interval] = event[ID_FIELD]
        interval.count -= 1
      else:
        heapq.heappop(heap)
        intervals_in_heap.remove(interval)
        # If interval is not yet exhausted or we need to select new dying
        # buckets, fetch more events.
        if (interval in self.current_dying_intervals or
            interval not in self.exhausted_intervals):
          self.current_dying_intervals -= self.exhausted_intervals
          intervals_fetched = self.fetch_events()
          if not intervals_fetched:
            continue
          for interval in intervals_fetched:
            if interval not in intervals_in_heap:
              events_for_interval = self.interval_to_events[interval]
              if not events_for_interval:
                continue
              event = events_for_interval.pop(0)
              heapq.heappush(heap, (CassandraSortedUUID(event[ID_FIELD]),
                                    (interval, event)))
              self.interval_to_latest_id[interval] = event[ID_FIELD]
              intervals_in_heap.add(interval)
              interval.count -= 1
    
class TimeWidthCassandraStorage(BaseStorage):
  EVENT_CF = 'events'
  INDEX_CF = 'index'
  MAX_WIDTH = 6*30*24*60*60 # 6 months. Too big, too small? Can we avoid it?
  CONF_PARAMETERS = {
    'timewidth_seconds': lambda x: int(x) <= TimeWidthCassandraStorage.MAX_WIDTH,
    'shards_per_bucket': int
  }  
  
  def __init__(self, name, **settings):
    self.hosts = settings['hosts']
    if not isinstance(self.hosts, list):
      raise ImproperlyConfigured('%s: `hosts` settings must be an array.' %
                                 self.__class__)
    self.keyspace = settings['keyspace']
    self.replication_factor = settings['replication_factor']
    self.default_width = settings['default_timewidth_seconds']
    if self.default_width > TimeWidthCassandraStorage.MAX_WIDTH:
      raise ImproperlyConfigured('%s: `default_wdith` settings must not exceed '
                                 'MAX_WIDTH [%s].' %
                                 (self.__class__,
                                  TimeWidthCassandraStorage.MAX_WIDTH))
    if not isinstance(self.default_width, int) or self.default_width < 0:
      raise ImproperlyConfigured('%s: `default_wdith` settings must be a '
                                 'postive integer.' %  self.__class__)    
    self.default_shards = settings['default_shards_per_bucket']
    if not isinstance(self.default_shards, int) or self.default_shards < 0:
      raise ImproperlyConfigured('%s: `default_shards` settings must be a '
                                 'postive integer.' %  self.__class__)
    self.read_size = int(settings['read_size'])
    self.setup_cassandra()
    self.index_cache = InMemoryLRUCache() # 1000-entry LRU cache.
    
  def setup_cassandra(self):
    self.system_manager = SystemManager(self.hosts[0])
    if self.keyspace not in self.system_manager.list_keyspaces():
      self.system_manager.create_keyspace(self.keyspace, SIMPLE_STRATEGY,
                                          {'replication_factor':
                                             str(self.replication_factor)})
      self.system_manager.create_column_family(
          self.keyspace, TimeWidthCassandraStorage.EVENT_CF,
          comparator_type=types.TimeUUIDType())
      self.system_manager.create_column_family(
          self.keyspace, TimeWidthCassandraStorage.INDEX_CF,
          # Column key: (start_time, width, shard).
          comparator_type=types.CompositeType(types.LongType(),
                                              types.IntegerType(),
                                              types.IntegerType()))
    self.pool = ConnectionPool(keyspace=self.keyspace, server_list=self.hosts)
    
  def is_alive(self):
    try:
      connection = self.pool.get()
      connection.describe_keyspace(self.keyspace) # Fake *ping* Cassandra?
      connection.return_to_pool()
      return True
    except:
      return False

  def floor_mod(self, value, mod):
    return int(math.floor(value / mod) * mod)

  def insert(self, stream, events, configuration):
    width = configuration.get('timewidth_seconds', self.default_width)
    shards = int(configuration.get('shards_per_bucket', self.default_shards))
    shard = random.randint(0, shards - 1)
    index_to_buckets = defaultdict(dict)
    bucket_to_events = defaultdict(dict)
    for event in events:
      bucket_start_time = self.floor_mod(event[TIMESTAMP_FIELD], width)
      bucket = '%s:%s:%s' % (stream, bucket_start_time, shard)
      bucket_to_events[bucket][UUID(event[ID_FIELD])] = cjson.encode(event)
      index_start_time = self.floor_mod(event[TIMESTAMP_FIELD], SECONDS_IN_YEAR)
      index = '%s:%s' % (stream, index_start_time)
      index_to_buckets[index][(bucket_start_time, width, shards)] = ''
    mutator = Mutator(self.pool, queue_size=1000)
    for bucket, events in bucket_to_events.iteritems():
      mutator.insert(ColumnFamily(self.pool,
                                  TimeWidthCassandraStorage.EVENT_CF),
                     bucket, events)
    for index, buckets in index_to_buckets.iteritems():
      try:
        self.index_cache.get(index)
      except KeyError:
        mutator.insert(ColumnFamily(self.pool,
                                    TimeWidthCassandraStorage.INDEX_CF),
                       index, buckets)
        self.index_cache.set(index, None)
    mutator.send()
    
  def _retrieve(self, stream, start_id, end_time, configuration):
    index = ColumnFamily(self.pool, TimeWidthCassandraStorage.INDEX_CF)
    start_time = (start_id.time - 0x01b21dd213814000L) * 100 / 1e9
    bucket_start_time = long(math.floor(
        max(start_time - TimeWidthCassandraStorage.MAX_WIDTH, 0)))
    assert(start_time >= bucket_start_time)
    bucket_start_width = int(math.ceil(start_time - bucket_start_time))
    end_time = long(math.ceil(end_time))
    indexes_to_scan = ['%s:%s' % (stream, i) for i in
                       range(self.floor_mod(bucket_start_time, SECONDS_IN_YEAR),
                             self.floor_mod(end_time, SECONDS_IN_YEAR) +
                             SECONDS_IN_YEAR,
                             SECONDS_IN_YEAR)]
    index_keys = index.multiget(indexes_to_scan,
                                column_start=(bucket_start_time,
                                              bucket_start_width, 0),
                                column_finish=(end_time, 0, 0),
                                buffer_size=len(indexes_to_scan))
    intervals = []
    for bucket_key in itertools.chain.from_iterable(index_keys.itervalues()):
      for i in xrange(bucket_key[2]):
        intervals.append(BucketInterval(stream, bucket_key, i))
    events = SortedShardedEventStream(
        ColumnFamily(self.pool, TimeWidthCassandraStorage.EVENT_CF),
        intervals,
        start_id, 
        convert_time_to_uuid(end_time, lowest_val=False), # Largest UUID.
        self.read_size)
    for event in events:
      yield event
