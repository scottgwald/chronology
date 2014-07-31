from collections import defaultdict

from kronos.common.time import epoch_time_to_kronos_time
from kronos.common.time import kronos_time_to_epoch_time
from kronos.conf import settings
from kronos.conf.constants import MAX_LIMIT
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.core import marshal
from kronos.storage.cassandra.internal import StreamShard
from kronos.storage.router import router
from kronos.utils.math import round_down
from kronos.utils.uuid import UUIDType
from kronos.utils.uuid import uuid_from_kronos_time
from tests.server import KronosServerTestCase


def uuid_from_time(time, uuid_type=UUIDType.RANDOM):
  return uuid_from_kronos_time(epoch_time_to_kronos_time(time), uuid_type)


class TestCassandraBackend(KronosServerTestCase):
  def setUp(self):
    super(TestCassandraBackend, self).setUp()
    self.namespace = router.get_backend('cassandra').namespaces['kronos']
    self.shards = settings.storage['cassandra']['shards_per_bucket']
    self.width_seconds = settings.storage['cassandra']['timewidth_seconds']
    self.width = epoch_time_to_kronos_time(self.width_seconds)

  def test_stream_sharding(self):
    ''' Inserts elements that fall into the same time bucket and checks if they
    are being sharded properly. '''
    
    stream_name = 'TestCassandraBackend_test_stream_sharding'
    stream = self.namespace.get_stream(stream_name, self.width, self.shards)
    
    # Since we have a small number of shards, let's just assume that if we
    # insert 100 events to the same bucket, we'll have inserted at least one
    # event in each shard.    
    for i in xrange(100):
      self.put(stream_name, [{TIMESTAMP_FIELD: epoch_time_to_kronos_time(1)}])
    events = self.get(stream_name, 0, epoch_time_to_kronos_time(2))
    self.assertEqual(len(events), 100)
      
    num_events = 0
    
    for shard in xrange(self.shards):
      stream_shard = StreamShard(stream.namespace, stream_name, 0, self.width,
                                 shard, False, MAX_LIMIT, 100)
      events = list(stream_shard.iterator(uuid_from_time(0),
                                          uuid_from_time(2)))
      self.assertTrue(len(events) > 0)
      num_events += len(events)
    # These 3 shards should contain all the events inserted into the stream in
    # the interval [0, 2).
    self.assertEqual(num_events, 100)

  def test_stream_splitting(self):
    ''' Inserts events that fall into multiple time buckets and ensures that
    each time bucket has the right number of events at the end and all the
    events in each bucket fall into its time range. '''
    
    stream_name = 'TestCassandraBackend_test_stream_splitting'
    stream = self.namespace.get_stream(stream_name, self.width, self.shards)
    settings.storage.cassandra.timewidth_seconds = 2
    router.reload()
    
    # Each bucket interval has width of 2 seconds, so this should distribute
    # events in 5 buckets: [0, 2), [2, 4), [4, 6), [6, 8), [8, 10).
    for i in xrange(100):
      self.put(stream_name, [{TIMESTAMP_FIELD:
                              epoch_time_to_kronos_time(i % 10)}])
    events = self.get(stream_name, 0, epoch_time_to_kronos_time(10))
    self.assertEqual(len(events), 100)
    
    bucket_to_events = defaultdict(list)
    for start_time in (0, 2, 4, 6, 8):
      # Fetch events directly from each bucket.
      for shard in xrange(self.shards):
        stream_shard = StreamShard(stream.namespace, stream_name,
                                   epoch_time_to_kronos_time(start_time),
                                   self.width, shard, False,
                                   MAX_LIMIT, 100)
        events = stream_shard.iterator(
          uuid_from_time(start_time, UUIDType.LOWEST),
          uuid_from_time(start_time + self.width_seconds))
        bucket_to_events[start_time].extend(marshal.loads(event.json)
                                            for event in events)

    num_events = 0
    for start_time, events in bucket_to_events.iteritems():
      # Each bucket should have 20 events and they must fall in the bucket's
      # time range.
      self.assertEqual(len(events), 20)
      for event in events:
        time = kronos_time_to_epoch_time(event[TIMESTAMP_FIELD])
        self.assertTrue(time >= start_time)
        self.assertTrue(time < start_time + self.width_seconds)
      num_events += len(events)
    self.assertEqual(num_events, 100)

  def test_index(self):
    ''' Inserts events in to a stream and ensures that the index column
    family contains all the of expected row keys. '''
    
    stream_name = 'TestCassandraBackend_test_index'
    stream = self.namespace.get_stream(stream_name, self.width, self.shards)
    
    for i in xrange(100):
      self.put(stream_name, [{TIMESTAMP_FIELD:
                              epoch_time_to_kronos_time(i % 20)}])
    events = self.get(stream_name, 0, epoch_time_to_kronos_time(20))
    self.assertEqual(len(events), 100)

    time_shards = defaultdict(set)
    for shard_desc in stream.get_overlapping_shards(
      0, epoch_time_to_kronos_time(20)):
      self.assertEqual(shard_desc['width'], self.width)
      time_shards[shard_desc['start_time']] |= {shard_desc['shard']}

    for shards in time_shards.itervalues():
      self.assertEqual(len(shards), self.shards)
      self.assertEqual(shards, set(xrange(self.shards)))

    self.assertEqual(set(time_shards),
                     set(round_down(epoch_time_to_kronos_time(t), self.width)
                         for t in xrange(20)))

  def test_overlapping_shards(self):
    ''' Tests that changing bucket widths doesn\'t break shit. First inserts
    events into two contiguous shards when the bucket width is 2 seconds. Then
    it changes bucket width to 4 and inserts the same events again, causing
    all events to go into a single bucket. At the end it checks if the first
    bucket has 3x the number of events as the seconds bucket. '''
    
    stream_name = 'TestCassandraBackend_test_overlapping_shards'
    stream = self.namespace.get_stream(stream_name, self.width, self.shards)
    
    for i in xrange(60):
      self.put(stream_name, [{TIMESTAMP_FIELD: epoch_time_to_kronos_time(i % 4)}])
    events = self.get(stream_name, 0, epoch_time_to_kronos_time(4))
    self.assertEqual(len(events), 60)

    # Change default width to be 4 seconds instead of 2.
    settings.storage.cassandra.timewidth_seconds = 4
    router.reload()

    # All of these events should now go into bucket with start time 0.
    for i in xrange(60):
      self.put(stream_name, [{TIMESTAMP_FIELD:
                              epoch_time_to_kronos_time(i % 4)}])
    events = self.get(stream_name, 0, epoch_time_to_kronos_time(4))
    self.assertEqual(len(events), 120)

    shard_to_events = defaultdict(int)
    for start_time in (0, 2):
      # Fetch events directly from each shard.
      for shard in xrange(self.shards):
        stream_shard = StreamShard(stream.namespace, stream_name,
                                   epoch_time_to_kronos_time(start_time),
                                   self.width, shard, False,
                                   MAX_LIMIT, 100)
        events = stream_shard.iterator(uuid_from_time(start_time,
                                                      UUIDType.LOWEST),
                                       uuid_from_time(start_time + 4))
        shard_to_events[start_time] += len(list(events))
    self.assertEqual(len(shard_to_events), 2)
    self.assertEqual(shard_to_events[0], 90) # 30 + 60.
    self.assertEqual(shard_to_events[2], 30) # 30 + 0.
    self.assertEqual(sum(shard_to_events.itervalues()), 120)

    # Revert default width settings.
    settings.storage.cassandra.timewidth_seconds = 2
    router.reload()
