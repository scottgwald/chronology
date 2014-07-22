from benchmarks import insert
from benchmarks import read
from benchmarks import timeit


def run():
  # For the stream "num_per_bucket" timewidth_seconds is 1000 and
  # num_shards is 1, so all of these inserts go into the same bucket. On each
  # iteration we adds 200k more events to the same bucket and then read 10k
  # events to see how much slower reading is now. Overall we insert 2m events
  # in batches on 200k each.
  for n in xrange(200000, 2200000, 200000):
    insert('num_per_bucket', 200000)
    timeit('n=%s' % n, read, 'num_per_bucket', 10000, start=500)
