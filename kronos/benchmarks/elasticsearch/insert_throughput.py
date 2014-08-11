import gevent

from benchmarks import insert
from benchmarks import timeit

def run():
  # Each event is ~181 bytes.
  for num_threads in (1, 5, 10):
    print '  * %s thread(s).' % num_threads
    for chunk_size in (500, 1000, 5000, 10000):
      stream = 'insert_%s' % chunk_size
      num_events_per_thread = 100000 / num_threads
      def execute():
        greenlets = [gevent.spawn(insert, stream, num_events_per_thread,
                                  chunk_size=chunk_size)
                     for _ in xrange(num_threads)]
        gevent.joinall(greenlets)
      timeit('chunk_size=%s' % chunk_size, execute)
