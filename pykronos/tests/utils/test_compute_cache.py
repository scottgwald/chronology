import functools
import unittest

from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from pykronos import KronosClient
from pykronos.client import TIMESTAMP_FIELD
from pykronos.utils.cache import QueryCache
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import timedelta_to_kronos_time

class ComputeCacheTest(unittest.TestCase):
  def setUp(self):
    self.client = KronosClient('http://localhost:9191/',
                               blocking=False,
                               sleep_block=0.2)
    self.total_events = 500
    self.computed_namespace = 'computed'
    self.increment = timedelta(minutes=1)
    self.start_time = datetime(2014, 6, 4, 22)
    self.bucket_width = timedelta(minutes=20)

  def compute_cache_test(function):
    """A wrapper that sets up a stream with test data.
    
    The stream takes the name of the function being run, and contains
    `self.total_events` events.  The events are each one
    `self.increment` apart.
    """
    @functools.wraps(function)
    def wrapper(self):
      self.stream = 'ComputeCacheTest_%s' % (function.__name__)
      for i in xrange(self.total_events):
        self.client.put({
          self.stream: [{TIMESTAMP_FIELD: 
                         self.start_time + (self.increment * i),
                         'a': i % 5, 'b': i}]})
      self.client.flush()
      function(self)
    return wrapper

  def filter_and_sum(self, start_time, end_time):
    """Bin `self.stream` into buckets, returning the sum of `b` when `a` == 2.

    For all events between `start_time` and `end_time`, create an
    event for every 20-minute interval of events that contains the sum
    of `b` when `a`==2.
    """
    events = self.client.get(self.stream, start_time, end_time)
    counts = defaultdict(int)
    grouping_minutes = timedelta_to_kronos_time(self.bucket_width)
    for event in events:
      if event['a'] == 2:
        counts[event['@time'] -
               (event['@time'] % grouping_minutes)] += event['b']
    for group_time in sorted(counts.iterkeys()):
      yield {'@time': group_time, 'b_sum': counts[group_time]}

  @compute_cache_test
  def test_cache_layer(self):
    cache = QueryCache(self.client, self.filter_and_sum, self.bucket_width,
                       self.computed_namespace)
    start_time = self.start_time - (self.bucket_width * 3)
    end_time = self.start_time + (self.total_events * self.increment) + (
      self.bucket_width * 3)
    untrusted_time = self.start_time + (
      timedelta(minutes=(self.total_events / 2) - 25))
    results = list(
      cache.compute_and_cache(start_time, end_time, untrusted_time))

    def verify_results(results, expected_results):
      self.client.flush()
      self.assertEqual(len(results), expected_results)
      result_time = self.start_time
      for idx, result in enumerate(results):
        self.assertEqual(result[TIMESTAMP_FIELD],
                         datetime_to_kronos_time(result_time))
        self.assertEqual(result['b_sum'], sum([2, 7, 12, 17] + [idx * 4 * (
                self.bucket_width.total_seconds() / 60)]))
        result_time += self.bucket_width

    # Verify all results were computed correctly.
    verify_results(results, 25)

    # Verify only trusted results are cached.
    results = list(cache.cached_results(start_time, end_time))
    verify_results(results, 11)

# TODO(marcua): more tests
# TODO(marcua): document all functions.
# TODO(marcua): document the class, including limitations.
