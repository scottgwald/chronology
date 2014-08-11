from benchmarks import insert
from benchmarks import read
from benchmarks import timeit


def run():
  for n in xrange(5000, 30000, 5000):
    stream = 'read_size_%d' % n
    insert(stream, 100000)
    timeit('read_size=%d' % n, read, stream, 100000)
