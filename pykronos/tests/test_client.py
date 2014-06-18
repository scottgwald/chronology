import functools
import time
import unittest

from datetime import datetime

from pykronos import KronosClient
from pykronos.client import TIMESTAMP_FIELD
from pykronos.utils.time import kronos_time_now


class KronosClientTest(unittest.TestCase):
  def setUp(self):
    self.blocking_client = KronosClient('http://localhost:9191/',
                                        blocking=True)
    self.nonblocking_client = KronosClient('http://localhost:9191/',
                                           blocking=False,
                                           sleep_block=0.2)
  def kronos_client_test(function):
    @functools.wraps(function)
    def wrapper(self):
      for i, client in enumerate([self.blocking_client,
                                  self.nonblocking_client]):
        self.stream = 'KronosClientTest_%s_%s' % (function.__name__, i)
        self.start_time = kronos_time_now()
        self.sleep_time = i * 0.4 # Will be 0 for blocking.
        self.client = client
        function(self)
    return wrapper

  @kronos_client_test
  def test_put_and_get(self):
    for i in xrange(2):
      stream = '%s_%s' % (self.stream, i)
      self.client.put({stream: [{'a': 1}, {'a': 2}]})
      self.client.put({stream: [{'a': 3}, {'a': 4}]})

    time.sleep(self.sleep_time)

    for i in xrange(2):
      stream = '%s_%s' % (self.stream, i)
      events = list(self.client.get(stream,
                                    self.start_time,
                                    kronos_time_now()))
      self.assertEqual(len(events), 4)
      self.assertEqual({1, 2, 3, 4}, set(map(lambda event: event['a'], events)))

    # Test with `datetime` timestamps.
    start_time = datetime.utcnow()
    for i in xrange(2):
      stream = '%s_%s' % (self.stream, i)
      self.client.put({stream: [{'a': 1, TIMESTAMP_FIELD: datetime.utcnow()},
                                {'a': 2, TIMESTAMP_FIELD: datetime.utcnow()}]})
      self.client.put({stream: [{'a': 3, TIMESTAMP_FIELD: datetime.utcnow()},
                                {'a': 4, TIMESTAMP_FIELD: datetime.utcnow()}]})

    time.sleep(self.sleep_time)

    for i in xrange(2):
      stream = '%s_%s' % (self.stream, i)
      events = list(self.client.get(stream,
                                    start_time,
                                    datetime.utcnow()))
      self.assertEqual(len(events), 4)
      self.assertEqual({1, 2, 3, 4}, set(map(lambda event: event['a'], events)))
    

  @kronos_client_test
  def test_delete(self):
    self.client.put({self.stream: [{'a': 1}, {'a': 2}]})
    mid_time = kronos_time_now()
    self.client.put({self.stream: [{'a': 3}, {'a': 4}]})

    time.sleep(self.sleep_time)

    events = list(self.client.get(self.stream,
                                  self.start_time,
                                  kronos_time_now()))
    self.assertEqual(len(events), 4)
    self.assertEqual({1, 2, 3, 4}, set(map(lambda event: event['a'], events)))

    response = self.client.delete(self.stream,
                                  mid_time,
                                  kronos_time_now())
    self.assertEqual(response[self.stream], {'memory': 2})

    events = list(self.client.get(self.stream,
                                  self.start_time,
                                  kronos_time_now()))
    self.assertEqual(len(events), 2)
    self.assertEqual({1, 2}, set(map(lambda event: event['a'], events)))    

  @kronos_client_test
  def test_get_streams(self):
    for i in xrange(10):
      stream = '%s_%s' % (self.stream, i)
      self.client.put({stream: [{i: None}]})

    time.sleep(self.sleep_time)

    streams = set(self.client.get_streams())

    for i in xrange(10):
      stream = '%s_%s' % (self.stream, i)
      self.assertTrue(stream in streams)

  @kronos_client_test
  def test_log_function(self):

    def getter_x(*args, **kwargs):
      return args[0]

    def getter_y(*args, **kwargs):
      return kwargs.get('y')

    def getter_z(*args, **kwargs):
      return args[0] * kwargs.get('y', 0)

    @self.client.log_function(self.stream,
                              properties={'x': getter_x,
                                          'y': getter_y,
                                          'z': getter_z})
    def my_function(x, y=None):
      return

    start = time.time()
    my_function(2, y=3)
    end = time.time()

    time.sleep(self.sleep_time)

    events = list(self.client.get(self.stream,
                                  self.start_time,
                                  kronos_time_now()))
    self.assertEqual(len(events), 1)
    event = events[0]
    self.assertEqual(event['x'], 2)
    self.assertEqual(event['y'], 3)
    self.assertEqual(event['z'], 6)
    self.assertTrue(event['duration'] < end - start )

  @kronos_client_test
  def test_log_scope(self):
    x = 1
    y = 2

    start = time.time()
    with self.client.log_scope(self.stream, properties={'x': x,
                                                        'y': y}) as event:
      event['hello'] = 'world'
      raise Exception('boom')
    end = time.time()

    time.sleep(self.sleep_time)

    events = list(self.client.get(self.stream,
                                  self.start_time,
                                  kronos_time_now()))
    self.assertEqual(len(events), 1)
    event = events[0]
    self.assertEqual(event['x'], 1)
    self.assertEqual(event['y'], 2)
    self.assertTrue(event['duration'] < end - start )
    self.assertEqual(event['hello'], 'world')
    self.assertTrue('exception' in event)
    self.assertEqual(event['exception']['class'], 'Exception')
    self.assertEqual(event['exception']['message'], 'boom')
