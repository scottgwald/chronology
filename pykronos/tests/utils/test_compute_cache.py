import functools
import unittest

from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from mock import patch
from pykronos import KronosClient
from pykronos.client import TIMESTAMP_FIELD
from pykronos.utils.cache import QueryCache
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import timedelta_to_kronos_time


class QueryCacheTest(unittest.TestCase):
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

  def verify_results(self, result_func, cache, expected_results,
                     expected_computations):
    with patch.object(cache, '_compute_and_cache_bucket',
                      wraps=cache._compute_and_cache_bucket) as mock_method:
      results = result_func()
      self.assertEqual(mock_method.call_count, expected_computations)

    self.assertEqual(len(results), expected_results)
    result_time = self.start_time
    for idx, result in enumerate(results):
      self.assertEqual(result[TIMESTAMP_FIELD],
                       datetime_to_kronos_time(result_time))
      self.assertEqual(result['b_sum'], sum([2, 7, 12, 17] + [idx * 4 * (
              self.bucket_width.total_seconds() / 60)]))
      result_time += self.bucket_width

  def test_cache_exceptions(self):
    # Bucket width shouldn't be more granular than 1 second.
    def bad_bucket_width():
      return QueryCache(self.client, self.filter_and_sum,
                        self.bucket_width + timedelta(milliseconds=1),
                        self.computed_namespace)
    self.assertRaises(ValueError, bad_bucket_width)

    # stat_time and end_time should align to bucket_width boundaries.
    cache = QueryCache(self.client, self.filter_and_sum,
                       self.bucket_width, self.computed_namespace)
    start_time = self.start_time - (self.bucket_width * 3)
    end_time = self.start_time + (self.total_events * self.increment) + (
      self.bucket_width * 3)
    untrusted_time = self.start_time + (
      timedelta(minutes=(self.total_events / 2) - 25))
    def bad_start_boundary():
      return list(
        cache.compute_and_cache(start_time + timedelta(minutes=1),
                                end_time, untrusted_time))
    self.assertRaises(ValueError, bad_start_boundary)

  @compute_cache_test
  def test_cache_layer(self):
    cache = QueryCache(self.client, self.filter_and_sum,
                       self.bucket_width, self.computed_namespace)
    start_time = self.start_time - (self.bucket_width * 3)
    end_time = self.start_time + (self.total_events * self.increment) + (
      self.bucket_width * 3)
    untrusted_time = self.start_time + (
      timedelta(minutes=(self.total_events / 2) - 25))

    # Verify all results were computed correctly.
    self.verify_results(lambda: list(
        cache.compute_and_cache(start_time, end_time, untrusted_time)),
                        cache, 25, 31)

    # Verify only trusted results are cached.
    self.verify_results(lambda: list(
        cache.cached_results(start_time, end_time)),
                        cache, 11, 0)

    # Running the same operations twice should result in the same
    # results as before.
    self.verify_results(lambda: list(
        cache.compute_and_cache(start_time, end_time, untrusted_time)),
                        cache, 25, 17)
    self.verify_results(lambda: list(
        cache.cached_results(start_time, end_time)),
                        cache, 11, 0)

    # Overlapping time queries should result in the same
    # results as before, and benefit from the cache.
    self.verify_results(lambda: list(
        cache.compute_and_cache(start_time - self.bucket_width,
                                end_time + self.bucket_width,
                                untrusted_time)),
                        cache, 25, 19)
    self.verify_results(lambda: list(
        cache.cached_results(start_time, end_time)),
                        cache, 11, 0)

    # Increasing the trusted time should increase the cached results.
    untrusted_time = untrusted_time + timedelta(minutes=40)
    self.verify_results(lambda: list(
        cache.compute_and_cache(start_time, end_time, untrusted_time)),
                        cache, 25, 17)
    self.verify_results(lambda: list(
        cache.cached_results(start_time, end_time)),
                        cache, 13, 0)

    # Decreasing trusted time shouldn't remove results.
    untrusted_time = untrusted_time - timedelta(minutes=40)
    self.verify_results(lambda: list(
        cache.compute_and_cache(start_time, end_time, untrusted_time)),
                        cache, 25, 15)
    self.verify_results(lambda: list(
        cache.cached_results(start_time, end_time)),
                        cache, 13, 0)

    # If there are two cached entries, that cached time should no
    # longer be returned.
    results = list(cache.cached_results(start_time, end_time))
    duplicate_result = dict(results[10])
    duplicate_result['b_sum'] = 0
    self.client.put({cache._scratch_stream:
                       [duplicate_result]},
                    namespace=cache._scratch_namespace)
    self.client.flush()
    safe_results = list(cache.cached_results(start_time, end_time))
    self.assertEqual(results[:10]+ results[11:], safe_results)

    # Rerunning the cache/computation should re-cache the corrupted
    # element.
    self.verify_results(lambda: list(
        cache.compute_and_cache(start_time, end_time, untrusted_time)),
                        cache, 25, 16)
    self.verify_results(lambda: list(
        cache.cached_results(start_time, end_time)),
                        cache, 13, 0)

    # Forcing computation should generate the same result set.
    self.verify_results(lambda: list(
        cache.compute_and_cache(start_time, end_time, untrusted_time,
                                force_compute=True)),
                        cache, 25, 31)
    self.verify_results(lambda: list(
        cache.cached_results(start_time, end_time)),
                        cache, 13, 0)
