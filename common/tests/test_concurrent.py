import functools
import gevent
import hashlib
import math
import random
import time
import unittest

from lib.concurrent import GreenletExecutor
from lib.concurrent import GIPCExecutor


def executor_test(function):
  @functools.wraps(function)
  def wrapper(self):
    for executor in self.executors:
      self.executor = executor
      function(self)
  return wrapper
      

class ExecutorTest(unittest.TestCase):
  def setUp(self):
    self.executors = [GreenletExecutor(num_greenlets=9),
                      GIPCExecutor(num_procs=3, num_greenlets=3)]

  def tearDown(self):
    map(lambda executor: executor.shutdown(), self.executors)

  @executor_test
  def test_correctness(self):
    for func in [lambda s: hashlib.md5(s).hexdigest(),
                 lambda s: s + 'world',
                 lambda s: len(s)]:
      result = self.executor.submit(func, ['hello']).get()
      self.assertEqual(result, func('hello'))

  @executor_test
  def test_async(self):
    func = lambda: gevent.sleep(0.2)
    # We have a concurrent factor of 9 for both executors so if we schedule
    # N `func` calls, we should have to wait for approximately
    # 0.2 * math.ceil(N/9.0) seconds.
    for i in [5, 9, 14]:
      start_time = time.time()
      results = [self.executor.submit(func) for _ in xrange(i)]
      self.executor.wait(results)
      duration = time.time() - start_time
      expected_time = 0.2 * math.ceil(i / 9.0)
      self.assertTrue(duration >= expected_time)
      self.assertTrue(duration <= expected_time + 0.1)

  @executor_test
  def test_wait(self):
    func = lambda x: gevent.sleep(0.1 * x)

    for _ in xrange(3):
      start_time = time.time()
      wait_num = random.choice(range(1, 10))
      # Fire off 9 tasks (so all can happen concurrently) where each
      # task takes (i * 0.1) seconds where i in [1, ..., 9].
      results = map(lambda i: self.executor.submit(func, [i]), range(1, 10))
      # Wait for a random N tasks, so we should take at most N * 0.1s
      # to unblock where N in [1, ..., 9].
      ready_results = self.executor.wait(results, num=wait_num)
      duration = time.time() - start_time
      self.assertTrue(duration >= wait_num * 0.1)
      self.assertTrue(duration <= (wait_num + 1) * 0.1)
      self.assertTrue(len(ready_results) >= wait_num)
      for result in ready_results:
        try:
          result.get(block=False)
        except gevent.Timeout:
          self.fail()
      self.executor.wait(results) # Wait for executor to be totally free.
