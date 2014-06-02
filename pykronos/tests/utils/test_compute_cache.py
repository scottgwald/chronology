import functools
import time
import unittest

from datetime import datetime

from pykronos import KronosClient
from pykronos.client import TIMESTAMP_FIELD
from pykronos.utils.time import kronos_time_now


class ComputeCacheTest(unittest.TestCase):
  def setUp(self):
    self.client = KronosClient('http://localhost:9191/',
                               blocking=False,
                               sleep_block=0.2)
    self.total_events = 500

  def compute_cache_test(function):
    """A wrapper that sets up a stream with test data.
    
    The stream takes the name of the function being run, and contains
    `self.total_events` events.  The events are each one minute apart.
    Half of them have an `@time` before kronos_time_now(), and the
    other half have an `@time` after kronos_time_now().
    """
    @functools.wraps(function)
    def wrapper(self):
      self.start_time = kronos_time_now() - timedelta_to_kronos_time(
        timedelta(minutes=self.total_events / 2))
      self.stream = 'ComputeCacheTest_%s' % (function.__name__)
      increment = timedelta_to_kronos_time(timedelta(minutes=1))
      for i in xrange(self.total_events):
        self.client.put({
          self.stream: [{'@time': self.start_time + (increment * i),
                         'a': i % 5, 'b': i}]})
      self.client.flush()
      function(self)
    return wrapper

  def filter_and_sum(start_time, end_time):
    """Bin `self.stream` into buckets, returning the sum of `b` when `a` == 2.

    For all events between `start_time` and `end_time`, create an
    event for every 20-minute interval of events that contains the sum
    of `b` when `a`==2.
    """
    events = self.client.get(self.stream, start_time, end_time)
    counts = defaultdict(int)
    grouping_minutes = timedelta_to_kronos_time(timedelta(minutes=20))
    for event in events:
      if event['a'] == 2:
        counts[event['@time'] -
               (event['@time'] % grouping_minutes)] += event['b']
    for group_time in sorted(counts.iterkeys()):
      yield {'@time': group_time, 'b_sum': counts[group_time]}

  @compute_cache_test
  def test_cache_layer(self):
    self.assertEqual({1, 2, 3, 4}, set(map(lambda event: event['a'], events)))
