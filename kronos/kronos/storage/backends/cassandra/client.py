"""
The Cassandra storage backend uses (stream_name, time interval) as
rowkeys and UUIDs as column names. UUIDs serve as precise timestamps, but also
differentiates two events that occured at the same time. This ensures that
Kronos will never store more than one event at a given (rowkey, column) pair.

# Storing an event

Suppose we want to store an event that occured at time `t` in a stream named
'clicks'.
  - The first part of the rowkey is 'clicks'.
  - From settings.py, we get the default width of a time interval `w`.  We
    find which of the intervals [0,w), [w,2w), ... that `t` lies in to construct
    the second part of the rowkey.
Also from settings.py, we get the sharding factor `s`. Kronos will spread the
load for a given rowkey across `s` different shards. For each event, Kronos
will pick one of the `s` shards at random to store the event.

The client should have included a UUID for the event based on the time and
the client's hardware address which Kronos will use as the column.

# Retrieving events

Now suppose we want to retrieve all events in a stream named `s` that occured
in a time interval [a,b). For each time interval `i` of [0,w), [2w,w), ...
that intersects [a,b) we read the row with key (`s`, `i`) from all shards.
Reading a row in Cassandra returns all (column, value) pairs in that row, and
we stream these values back to the caller in UUID order. Note that we may need
to prune some events out of this stream if they occured in an intersecting
interval but <a or >=b.

# Indices

Indices help determine which buckets need to be scanned to retrieve events
between a given start and end time.
TODO(meelap): Finish documenting indexes.
"""

# TODO(usmanm): Cython some code to speed it?
import itertools
import logging
import json
import random

from collections import defaultdict
from datetime import timedelta
from pycassa import ColumnFamily
from pycassa import ConnectionPool
from pycassa import ConsistencyLevel
from pycassa import types
from pycassa.batch import Mutator
from pycassa.system_manager import SIMPLE_STRATEGY, SystemManager
from thrift.transport.TTransport import TTransportException
from uuid import UUID

from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import ResultOrder
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.core.exceptions import ConnectionFailure
from kronos.storage.backends import BaseStorage
from kronos.storage.backends.cassandra.datastructures import BucketInterval
from kronos.storage.backends.cassandra.datastructures import SortedShardedEventStream
from kronos.utils.cache import InMemoryLRUCache
from kronos.utils.math import round_down
from kronos.utils.math import time_to_kronos_time
from kronos.utils.math import uuid_from_kronos_time
from kronos.utils.math import uuid_to_kronos_time
from kronos.utils.math import UUIDType

log = logging.getLogger(__name__) 

LENGTH_OF_YEAR = int(timedelta(days=365.25).total_seconds() * 1e7)



class Namespace(object):
  EVENT_CF = 'events'
  INDEX_CF = 'index'

  def __init__(self, system_manager, keyspace_name, replication_factor,
               hosts=None):
    self.system_manager = system_manager
    self.keyspace_name = keyspace_name
    self.replication_factor = replication_factor;
    self.hosts = hosts or [system_manager._conn.server]
    self.setup()

  def setup(self):
    if self.keyspace_name not in self.system_manager.list_keyspaces():
      self.system_manager.create_keyspace(
          self.keyspace_name,
          SIMPLE_STRATEGY,
          {'replication_factor': str(self.replication_factor)})
      self.system_manager.create_column_family(
          self.keyspace_name,
          Namespace.EVENT_CF,
          comparator_type=types.TimeUUIDType())
      self.system_manager.create_column_family(
          self.keyspace_name,
          Namespace.INDEX_CF,
          # Column key: (start_time, width, shard).
          comparator_type=types.CompositeType(types.LongType(),
                                              types.IntegerType(),
                                              types.IntegerType()))
    self.pool = ConnectionPool(keyspace=self.keyspace_name,
                               server_list=self.hosts)
    self.index_cf = ColumnFamily(self.pool, Namespace.INDEX_CF)
    self.event_cf = ColumnFamily(self.pool, Namespace.EVENT_CF)
    # Index cache is a write cache: it prevents us from writing to the
    # bucket index if we've already updated it in a previous
    # operation.
    self.index_cache = InMemoryLRUCache(max_items=1000)

  def _drop(self):
    self.system_manager.drop_keyspace(self.keyspace_name)


class TimeWidthCassandraStorage(BaseStorage):
  INDEX_WIDTH = LENGTH_OF_YEAR
  MAX_WIDTH = INDEX_WIDTH / 2 # Too big, small, necessary?

  SETTINGS_VALIDATORS = {
    'default_timewidth_seconds': 
         lambda x: (int(x) > 0 and 
                    time_to_kronos_time(int(x)) <=
                      TimeWidthCassandraStorage.MAX_WIDTH),
    'default_shards_per_bucket': lambda x: int(x) > 0,
    'hosts': lambda x: isinstance(x, list),
    'keyspace_prefix': lambda x: len(str(x)) > 0,
    'replication_factor': lambda x: int(x) >= 1,
    'read_size': lambda x: int(x)
  }

  def __init__(self, name, namespaces=None, **settings):
    """
    Check that settings contains all of the required parameters in the right
    format, then setup a connection to the specified Cassandra instance.
    """
    super(TimeWidthCassandraStorage, self).__init__(name, **settings)
    self.namespaces = dict()
    self.setup_cassandra(namespaces)
    
  def setup_cassandra(self, namespaces):
    """
    Set up a connection pool to the specified Cassandra instances and create the
    specified keyspace if it does not exist.
    """
    for host in self.hosts:
      try:
        self.system_manager = SystemManager(host)
        break
      except TTransportException:
        log.error('Error creating system manager', exc_info=True)
    else:
      raise ConnectionFailure('No hosts available.')

    for namespace_name in namespaces:
      # Shit, this looks like such a baller smiley.
      keyspace = '{}_{}'.format(self.keyspace_prefix, namespace_name)
      namespace = Namespace(self.system_manager, keyspace,
                            self.replication_factor, hosts=self.hosts)
      self.namespaces[namespace_name] = namespace
    
  def is_alive(self):
    """
    Is our connection to Cassandra alive?
    """
    try:
      self.system_manager.list_keyspaces()
      return True
    except:
      return False

  def _insert(self, namespace, stream, events, configuration):
    """
    Store the specified events for the named stream.
    `stream` : The name of a stream.
    `events` : A list of events to store for the named stream.
    `configuration` : A dictionary of settings to override any default settings,
                      such as number of shards or width of a time interval.
    """
    namespace = self.namespaces[namespace]
    width = configuration.get('timewidth_seconds',
                              self.default_timewidth_seconds)
    width = time_to_kronos_time(width)
    shards = int(configuration.get('shards_per_bucket',
                                   self.default_shards_per_bucket))
    shard = random.randint(0, shards - 1)

    # 'stream:index_start' => [(stream, bucket_start, shard)]
    index_to_buckets = defaultdict(dict)
    # (stream, bucket_start, shard) => {id => properties}
    bucket_to_events = defaultdict(dict)

    # Group together all events that are in the same bucket so that
    # bucket_to_events maps bucketnames to { column_name==UUID : event, ... }.
    for event in events:
      bucket_start_time = round_down(event[TIMESTAMP_FIELD], width)
      bucket_name = BucketInterval.name(stream, bucket_start_time, shard)
      bucket_to_events[bucket_name][UUID(event[ID_FIELD])] = json.dumps(event)
      index_start_time = round_down(event[TIMESTAMP_FIELD], LENGTH_OF_YEAR)
      index = '%s:%s' % (stream, index_start_time)
      index_to_buckets[index][(bucket_start_time, width, shards)] = ''

    # Batch and wait on all index writes.
    mutator = Mutator(namespace.pool, queue_size=1000)
    index_updates = {}
    for index, buckets in index_to_buckets.iteritems():
      buckets_set = set(buckets)
      try:
        cached_index_value = index_updates.get(index,
                                               namespace.index_cache.get(index))
        new_buckets_set = buckets_set - cached_index_value
        if new_buckets_set:
          # Write the new buckets covered by this index entry.
          mutator.insert(namespace.index_cf, index,
                         dict.fromkeys(new_buckets_set, ''))
          index_updates[index] = buckets_set | cached_index_value
      except KeyError:
        # If we haven't written to this index before, write which
        # buckets have been updated.
        mutator.insert(namespace.index_cf, index, buckets)
        index_updates[index] = buckets_set
    mutator.send(ConsistencyLevel.ALL, atomic=True)

    # Cache the indices whose buckets we've updated.
    for index, buckets in index_updates.iteritems():
      namespace.index_cache.set(index, buckets)

    # Add all event writes to the batch of operations.
    for bucket_name, events in bucket_to_events.iteritems():
      mutator.insert(namespace.event_cf, bucket_name, events)
    mutator.send()

  def _delete(self, namespace, stream, start_id, end_time, configuration):
    """
    Delete events for `stream` between `start_id` and `end_time`.
    `stream` : The stream to delete events for.
    `start_id` : Delete events with id > `start_id`.
    `end_time` : Delete events ending <= `end_time`.
    `configuration` : A dictionary of settings to override any default
                      settings, such as number of shards or width of a
                      time interval.
    """
    namespace = self.namespaces[namespace]
    start_time = uuid_to_kronos_time(start_id)
    bucket_start_time = max(start_time - TimeWidthCassandraStorage.MAX_WIDTH, 0)
    bucket_start_width = start_time - bucket_start_time
    indexes_to_scan = ['%s:%s' % (stream, i) for i in
                       xrange(round_down(bucket_start_time, LENGTH_OF_YEAR),
                              round_down(end_time, LENGTH_OF_YEAR) +
                              LENGTH_OF_YEAR,
                              LENGTH_OF_YEAR)]
    index_keys = namespace.index_cf.multiget(indexes_to_scan,
                                             column_start=(bucket_start_time,
                                                           bucket_start_width,
                                                           0),
                                             column_finish=(end_time, 0, 0),
                                             buffer_size=len(indexes_to_scan))
    end_id = uuid_from_kronos_time(end_time, _type=UUIDType.HIGHEST)
    events_deleted = 0
    cf_mutator = namespace.event_cf.batch(queue_size=1000)
    # TODO(usmanm): What if all events in the bucket are removed? Should we
    # delete the bucket row and entry in the index CF?
    intervals = dict()
    for bucket_key in itertools.chain.from_iterable(index_keys.itervalues()):
      for shard in xrange(bucket_key[2]):
        bucket_interval = BucketInterval(namespace.event_cf, stream, bucket_key,
                                         shard, ResultOrder.ASCENDING,
                                         self.read_size)
        name = bucket_interval.name
        if (getattr(intervals.get(name), 'end', -float('inf')) >=
            bucket_interval.end):
          continue
        intervals[name] = bucket_interval
    for row_key, bucket_interval in intervals.iteritems():
      events = [event.id for event in bucket_interval.iterator(start_id, end_id)
                if event.id != start_id]
      if not events:
        continue
      cf_mutator.remove(row_key, events)
      events_deleted += len(events)
    cf_mutator.send()
    return events_deleted
  
  def _retrieve(self, namespace, stream, start_id, end_time, order, limit,
                configuration):
    """
    Retrieve events for `stream` between `start_id` and `end_time`.
    `stream` : The stream to return events for.
    `start_id` : Return events with id > `start_id`.
    `end_time` : Return events ending <= `end_time`.
    `order` : Whether to return the results in ResultOrder.ASCENDING
              or ResultOrder.DESCENDING time-order.
    `configuration` : A dictionary of settings to override any default
                      settings, such as number of shards or width of a
                      time interval.
    """
    namespace = self.namespaces[namespace]
    
    # Time of the first event to return
    start_time = uuid_to_kronos_time(start_id)

    # Time of the oldest bucket that could possibly contain the first event.
    bucket_start_time = max(start_time - TimeWidthCassandraStorage.MAX_WIDTH, 0)

    # Smallest possible width of the oldest bucket that could possibly contain
    # the first event.
    bucket_start_width = int(start_time - bucket_start_time)

    # Index width is one year. Get all indices pointing to buckets which
    # intersect with our time interval of interest.
    indexes_to_scan = ['%s:%s' % (stream, i) for i in
                       range(round_down(bucket_start_time, LENGTH_OF_YEAR),
                             round_down(end_time, LENGTH_OF_YEAR) +
                             LENGTH_OF_YEAR,
                             LENGTH_OF_YEAR)]

    # Get all buckets which might contain events of interest.
    index_keys = namespace.index_cf.multiget(indexes_to_scan,
                                             column_start=(bucket_start_time,
                                                           bucket_start_width,
                                                           0),
                                             column_finish=(end_time, 0, 0),
                                             buffer_size=len(indexes_to_scan))

    # Construct a list of BucketIntervals to scan for matching events.
    intervals = dict()
    for bucket_key in itertools.chain.from_iterable(index_keys.itervalues()):
      for i in xrange(bucket_key[2]):
        bucket_interval = BucketInterval(namespace.event_cf, stream, bucket_key,
                                         i, order, self.read_size)
        name = bucket_interval.name
        if (getattr(intervals.get(name), 'end', -float('inf')) >=
            bucket_interval.end):
          continue
        intervals[name] = bucket_interval

    end_id = uuid_from_kronos_time(end_time, _type=UUIDType.HIGHEST)
    events = SortedShardedEventStream(intervals.values(), start_id, end_id,
                                      limit, order)
    try:
      events = events.__iter__()
      event = events.next()
      # If first event's ID is equal to `start_id`, skip it.
      if event[ID_FIELD] != str(start_id):
        yield event
      while True:
        yield events.next()
    except StopIteration:
      pass

  def _streams(self, namespace):
    # TODO(usmanm): Ideally, we don't want to keep an in-memory set of all
    # stream names because it could cause memory issues. How to dedup?
    namespace = self.namespaces[namespace]
    streams = set()
    for index in namespace.index_cf.get_range(column_count=0,
                                              filter_empty=False):
      stream = index[0].split(':')[0]
      if stream not in streams:
        streams.add(stream)
        yield stream
