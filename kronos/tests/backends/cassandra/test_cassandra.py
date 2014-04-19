from collections import defaultdict

from kronos.conf import settings
from kronos.conf.constants import ResultOrder
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.storage import router
from kronos.storage.backends.cassandra.client import BucketInterval
from kronos.storage.backends.cassandra.client import TimeWidthCassandraStorage
from kronos.utils.math import kronos_time_to_time
from kronos.utils.math import time_to_kronos_time
from kronos.utils.math import UUIDType
from kronos.utils.math import uuid_from_kronos_time
from tests.server import KronosServerTestCase


def uuid_from_time(time, uuid_type=UUIDType.RANDOM):
  return uuid_from_kronos_time(time_to_kronos_time(time), uuid_type)


class TestCassandraBackend(KronosServerTestCase):
  def setUp(self):
    super(TestCassandraBackend, self).setUp()
    self.backend = router.get_backend('cassandra')
    self.namespace = self.backend.namespaces['kronos']
    self.shards = settings.storage['cassandra']['default_shards_per_bucket']
    self.width_seconds = (settings.storage['cassandra']
                          ['default_timewidth_seconds'])
    self.width = time_to_kronos_time(self.width_seconds)

  def test_bucket_shards(self):
    ''' Inserts elements that fall into the same time bucket and checks if they
    are being sharded properly. '''
    
    stream = 'TestCassandraBackend_test_bucket_shards'
    # Since we have a small number of shards, let's just assume that if we
    # insert 100 events to the same bucket, we'll have inserted at least one
    # event in each shard.
    for i in xrange(100):
      self.put(stream, [{TIMESTAMP_FIELD: time_to_kronos_time(1)}])
    events = self.get(stream, 0, time_to_kronos_time(2))
    self.assertEqual(len(events), 100)

    num_events = 0
    for shard in xrange(self.shards):
      bucket_interval = BucketInterval(self.namespace.event_cf, stream,
                                       (0, self.width), shard,
                                       ResultOrder.ASCENDING, 5)
      events = list(bucket_interval.iterator(uuid_from_time(0, UUIDType.LOWEST),
                                          uuid_from_time(2)))
      self.assertTrue(len(events) > 0)
      num_events += len(events)
    # These 3 shards should contain all the events inserted into the stream in
    # the interval [0, 2).
    self.assertEqual(num_events, 100)

  def test_bucket_intervals(self):
    ''' Inserts events that fall into multiple time buckets and ensures that
    each time bucket has the right number of events at the end and all the
    events in each bucket fall into its time range. '''
    
    stream = 'TestCassandraBackend_test_bucket_intervals'
    # Each bucket interval has width of 2 seconds, so this should distribute
    # events in 5 buckets: [0, 2), [2, 4), [4, 6), [6, 8), [8, 10).
    for i in xrange(100):
      self.put(stream, [{TIMESTAMP_FIELD: time_to_kronos_time(i % 10)}])
    events = self.get(stream, 0, time_to_kronos_time(10))
    self.assertEqual(len(events), 100)
    
    bucket_to_events = defaultdict(list)
    for start_time in (0, 2, 4, 6, 8):
      # Fetch events directly from each bucket.
      for shard in xrange(self.shards):
        bucket_interval = BucketInterval(
          self.namespace.event_cf, stream,
          (time_to_kronos_time(start_time), self.width), shard,
          ResultOrder.ASCENDING, 5)
        events = bucket_interval.iterator(
          uuid_from_time(start_time, UUIDType.LOWEST),
          uuid_from_time(start_time + self.width_seconds))
        bucket_to_events[start_time].extend(event.raw_event
                                            for event in events)
    num_events = 0
    for start_time, events in bucket_to_events.iteritems():
      # Each bucket should have 20 events and they must fall in the bucket's
      # time range.
      self.assertEqual(len(events), 20)
      for event in events:
        time = kronos_time_to_time(event[TIMESTAMP_FIELD])
        self.assertTrue(time >= start_time)
        self.assertTrue(time < start_time + self.width_seconds)
      num_events += len(events)
    self.assertEqual(num_events, 100)

  def test_index(self):
    ''' Inserts events in buckets that fall into two separate index row (i.e.
    events fall into two separate years). Then ensures that the index column
    family contains two rows and each row has the right number of buckets. It
    also ensures that the index cache is in sync with the persistent index. '''
    
    stream = 'TestCassandraBackend_test_index'
    # The outer loop ensures that we create two different index rows.
    for i in (0, TimeWidthCassandraStorage.INDEX_WIDTH):
      for j in xrange(50):
        self.put(stream, [{TIMESTAMP_FIELD: i + time_to_kronos_time(j % 4)}])
    events = self.get(
      stream, 0, TimeWidthCassandraStorage.INDEX_WIDTH + time_to_kronos_time(4))
    self.assertEqual(len(events), 100)

    index_keys = self.namespace.index_cf.multiget(
      ['%s:%s' % (stream, i) for i in (0,
                                       TimeWidthCassandraStorage.INDEX_WIDTH)])
    self.assertEqual(len(index_keys), 2)
    for index_key, buckets in index_keys.iteritems():
      index_start = int(index_key.split(':')[1])
      self.assertEqual(len(buckets), 2)
      for i, bucket in enumerate(buckets):
        if i == 0:
          self.assertEqual(index_start, bucket[0])
        else:
          self.assertEqual(index_start + time_to_kronos_time(2), bucket[0])
        self.assertEqual(bucket[1], self.width) # 2 second width.
        self.assertEqual(bucket[2], self.shards) # 3 shards per bucket.

    # Ensure cached index state matched persistent index state.
    for key, buckets in index_keys.iteritems():
      try:
        cached_buckets = self.namespace.index_cache.get(key)
      except KeyError:
        self.fail()
      self.assertEqual(set(buckets), cached_buckets)

  def test_overlapping_buckets(self):
    ''' Tests that changing bucket widths doesn\'t break shit. First inserts
    events into two contiguous buckets when the bucket width is 2 seconds. Then
    it changes bucket width to 4 and inserts the same events again, causing
    all events to go into a single bucket. At the end it checks if the first
    bucket has 3x the number of events as the seconds bucket. '''
    
    stream = 'TestCassandraBackend_test_overlapping_buckets'
    for i in xrange(60):
      self.put(stream, [{TIMESTAMP_FIELD: time_to_kronos_time(i % 4)}])
    events = self.get(stream, 0, time_to_kronos_time(4))
    self.assertEqual(len(events), 60)

    # Change default width to be 4 seconds instead of 2.
    self.backend.default_timewidth_seconds = time_to_kronos_time(4)

    # All of these events should now go into bucket with start time 0.
    for i in xrange(60):
      self.put(stream, [{TIMESTAMP_FIELD: time_to_kronos_time(i % 4)}])
    events = self.get(stream, 0, time_to_kronos_time(4))
    self.assertEqual(len(events), 120)

    bucket_to_events = defaultdict(list)
    for start_time in (0, 2):
      # Fetch events directly from each bucket.
      for shard in xrange(self.shards):
        bucket_interval = BucketInterval(
          self.namespace.event_cf, stream,
          (time_to_kronos_time(start_time), self.width), shard,
          ResultOrder.ASCENDING, 5)
        events = bucket_interval.iterator(
          uuid_from_time(start_time, UUIDType.LOWEST),
          uuid_from_time(start_time + 4))
        bucket_to_events[start_time].extend(events)
    self.assertEqual(len(bucket_to_events), 2)
    self.assertEqual(len(bucket_to_events[0]), 90) # 30 + 60.
    self.assertEqual(len(bucket_to_events[2]), 30) # 30 + 0.
    self.assertEqual(sum(map(len, bucket_to_events.itervalues())), 120)

    # Revert default width settings.
    self.backend.default_timewidth_seconds = time_to_kronos_time(2)
