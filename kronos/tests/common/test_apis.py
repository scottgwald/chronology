import random
import time

from collections import defaultdict

from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import ResultOrder
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.utils.math import time_to_kronos_time
from tests.server import KronosServerTestCase


class TestKronosAPIs(KronosServerTestCase):
  def test_put(self):
    # Test put with increasing timestamps.
    stream = 'TestKronosAPIs_test_put_ascending'
    for t in xrange(1, 11):
      self.put(stream, [{'@time': t}])
    events = self.get(stream, 0, 10)
    self.assertEqual(len(events), 10)

    # Test put with random timestamps.
    stream = 'TestKronosAPIs_test_put_random'
    for t in xrange(1, 11):
      self.put(stream, [{'@time': random.randint(1, 10)}])
    events = self.get(stream, 0, 10)
    self.assertEqual(len(events), 10)

    # Test put with decreasing timestamps.
    stream = 'TestKronosAPIs_test_put_decreasing'
    for t in xrange(10, 0, -1):
      self.put(stream, [{'@time': t}])
    events = self.get(stream, 0, 10)
    self.assertEqual(len(events), 10)

    # Test put with multiple events.
    stream = 'TestKronosAPIs_test_put_multiple_events'
    self.put(stream, [{TIMESTAMP_FIELD: random.randint(1, 10)}
                      for i in xrange(10)])
    events = self.get(stream, 0, 10)
    self.assertEqual(len(events), 10)

    # Test put with mapping.    
    mapping = defaultdict(list)
    for stream in ('TestKronosAPIs_test_put_mapping_1',
                   'TestKronosAPIs_test_put_mapping_2',
                   'TestKronosAPIs_test_put_mapping_3'):
      mapping[stream] = [{TIMESTAMP_FIELD: random.randint(1, 10)}
                         for i in xrange(10)]
    self.put(mapping)
    for stream in ('TestKronosAPIs_test_put_mapping_1',
                   'TestKronosAPIs_test_put_mapping_2',
                   'TestKronosAPIs_test_put_mapping_3'):
      events = self.get(stream, 0, 10)
      self.assertEqual(len(events), 10)    

  def test_get(self):
    stream = 'TestKronosAPIs_test_get'
    event1 = [{'a': 1, TIMESTAMP_FIELD: 1}]
    event2 = [{'a': 2, TIMESTAMP_FIELD: 2}]
    event3 = [{'a': 3, TIMESTAMP_FIELD: 3}]
    event4 = [{'a': 4, TIMESTAMP_FIELD: 3}]

    # Test get from non-existent streams.
    events = self.get(stream, 0, 4)
    self.assertEqual(len(events), 0)

    # Test get with intervals that have and don't have events.
    self.put(stream, event1)
    events = self.get(stream, 0, 4)
    self.assertEqual(len(events), 1)
    events = self.get(stream, 2, 4)
    self.assertEqual(len(events), 0)

    # Test get with different time slices.
    self.put(stream, event2)
    events = self.get(stream, 0, 4)
    self.assertEqual(len(events), 2)
    events = self.get(stream, 2, 4)
    self.assertEqual(len(events), 1)
    event2_id = events[0][ID_FIELD]

    self.put(stream, event3)
    events = self.get(stream, 0, 4)
    self.assertEqual(len(events), 3)
    events = self.get(stream, 2, 4)
    self.assertEqual(len(events), 2)

    # Test get for overlapping time events.
    self.put(stream, event4)
    events = self.get(stream, 0, 4)
    self.assertEqual(len(events), 4)
    events = self.get(stream, 2, 4)
    self.assertEqual(len(events), 3)
    events = self.get(stream, 3, 4)
    self.assertEqual(len(events), 2)

    # Test get for `start_time` and `end_time` inclusivity.
    events = self.get(stream, 1, 3)
    self.assertEqual(len(events), 4)

    # Test get with `start_id`.
    events = self.get(stream, None, 4, start_id=event2_id)
    self.assertEqual(len(events), 2)
    for event in events:
      self.assertEqual(event[TIMESTAMP_FIELD], 3)

    # Test get with `limit`.
    events = self.get(stream, 0, 4, limit=2)
    self.assertEqual(len(events), 2)    
    self.assertEqual(events[0][TIMESTAMP_FIELD], 1)
    self.assertEqual(events[1][TIMESTAMP_FIELD], 2)
    events = self.get(stream, 0, 4, limit=3)
    self.assertEqual(len(events), 3)
    self.assertEqual(events[0][TIMESTAMP_FIELD], 1)
    self.assertEqual(events[1][TIMESTAMP_FIELD], 2)
    self.assertEqual(events[2][TIMESTAMP_FIELD], 3)
    events = self.get(stream, 0, 4, limit=0)
    self.assertEqual(len(events), 0)

    # Test get with `order`.
    events = self.get(stream, 0, 4, order=ResultOrder.ASCENDING)
    self.assertEqual(len(events), 4)
    max_time = float('-inf')
    for event in events:
      self.assertTrue(event[TIMESTAMP_FIELD] >= max_time)
      max_time = event[TIMESTAMP_FIELD]
    events = self.get(stream, 0, 4, order=ResultOrder.DESCENDING)
    self.assertEqual(len(events), 4)
    min_time = float('inf')
    for event in events:
      self.assertTrue(event[TIMESTAMP_FIELD] <= min_time)
      min_time = event[TIMESTAMP_FIELD]

    # Test get with weird time ranges.
    # `start_time` == `end_time`
    self.assertEqual(len(self.get(stream, 3, 3)), 2)
    self.assertEqual(len(self.get(stream, 4, 4)), 0)

    # `start_time` and `end_time` in the future.
    now = time_to_kronos_time(time.time())
    self.assertEqual(len(self.get(stream,
                                  now + time_to_kronos_time(1000),
                                  now + time_to_kronos_time(2000))), 0)
    # `start_time` > `end_time`
    self.assertEqual(len(self.get(stream, 10, 5)), 0)

    # `start_time` < 0 and `end_time` < 0
    self.assertEqual(len(self.get(stream, -2000, -1000)), 0)

  def test_delete(self):
    stream = 'TestKronosAPIs_test_delete'
    event1 = [{'a': 1, TIMESTAMP_FIELD: 1}]
    event2 = [{'a': 3, TIMESTAMP_FIELD: 2}]
    event3 = [{'a': 4, TIMESTAMP_FIELD: 2}]

    # Test delete from non-existent streams.
    num_deleted = self.delete(stream, 0, 4)
    for num in num_deleted[stream].itervalues():
      self.assertEqual(num, 0)

    # Test delete with intervals that have and don't have events.
    self.put(stream, event1)
    num_deleted = self.delete(stream, 2, 4)
    for num in num_deleted[stream].itervalues():
      self.assertEqual(num, 0)
    self.assertEqual(len(self.get(stream, 0, 4)), 1)
    num_deleted = self.delete(stream, 0, 1)
    for num in num_deleted[stream].itervalues():
      self.assertEqual(num, 1)
    self.assertEqual(len(self.get(stream, 0, 4)), 0)

    # Test delete overlapping time events.
    self.put(stream, event2 + event3)
    self.assertEqual(len(self.get(stream, 0, 4)), 2)
    num_deleted = self.delete(stream, 2, 2)
    for num in num_deleted[stream].itervalues():
      self.assertEqual(num, 2)
    self.assertEqual(len(self.get(stream, 0, 4)), 0)

    # Test delete with `start_id`.
    self.put(stream, event2 + event3)
    start_id = self.get(stream, 0, 4, limit=1)[0][ID_FIELD]
    self.assertEqual(len(self.get(stream, 0, 4)), 2)
    num_deleted = self.delete(stream, None, 2, start_id=start_id)
    for num in num_deleted[stream].itervalues():
      self.assertEqual(num, 1)
    events = self.get(stream, 0, 4)
    self.assertEqual(len(events), 1)
    self.assertEqual(events[0][ID_FIELD], start_id)    

  def test_streams(self):
    streams = {}
    for i in range(10):
      n = random.randint(1, 1000)
      stream = 'TestKronosAPIs_test_streams_{}'.format(n)
      self.put(stream, [{'@time': n, n: None, 'lol': 'cat'}])
      streams[stream] = n
    retrieved_streams = {stream for stream in self.get_streams()
                         if stream.startswith('TestKronosAPIs_test_streams_')}
    self.assertEqual(len(retrieved_streams), 10)
    self.assertEqual(retrieved_streams, set(streams.iterkeys()))

  def test_namespaces(self):
    namespace1 = 'namespace1'
    namespace2 = 'namespace2'
    stream = 'TestKronosAPIs_test_namespaces'
    events1 = [{'ns': 1, TIMESTAMP_FIELD: 1},
               {'ns': 1, TIMESTAMP_FIELD: 2},
               {'ns': 1, TIMESTAMP_FIELD: 10}]
    events2 = [{'ns': 2, TIMESTAMP_FIELD: 2},
               {'ns': 2, TIMESTAMP_FIELD: 5},
               {'ns': 2, TIMESTAMP_FIELD: 7}]

    # Check that both namespace1 and namespace2 are empty.
    self.assertEqual(len(self.get_streams(namespace=namespace1)), 0)
    self.assertEqual(len(self.get_streams(namespace=namespace2)), 0)

    # Put events into namespace1.
    self.put(stream, events1, namespace=namespace1)
    self.assertEqual(len(self.get_streams(namespace=namespace1)), 1)
    self.assertEqual(len(self.get_streams(namespace=namespace2)), 0)
    self.assertEqual(len(self.get(stream, 0, 10, namespace=namespace1)), 3)
    self.assertEqual(len(self.get(stream, 0, 10, namespace=namespace2)), 0)
    self.assertTrue(all(e['ns'] == 1
                        for e in self.get(stream, 0, 10, namespace=namespace1)))

    # Put events into namespace2.
    self.put(stream, events2, namespace=namespace2)
    self.assertEqual(len(self.get_streams(namespace=namespace1)), 1)
    self.assertEqual(len(self.get_streams(namespace=namespace2)), 1)
    self.assertEqual(len(self.get(stream, 0, 10, namespace=namespace1)), 3)
    self.assertEqual(len(self.get(stream, 0, 10, namespace=namespace2)), 3)
    self.assertTrue(all(e['ns'] == 1
                        for e in self.get(stream, 0, 10, namespace=namespace1)))
    self.assertTrue(all(e['ns'] == 2
                        for e in self.get(stream, 0, 10, namespace=namespace2)))
