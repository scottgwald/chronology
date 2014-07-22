from benchmarks import insert
from benchmarks import read
from benchmarks import timeit


def run():
  for n in (1, 3, 6, 12, 24):
    stream = 'shards_%s' % n
    insert(stream, 200000)
    timeit('shards=%s' % n, read, stream, 150000)
