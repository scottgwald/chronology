import time
import uuid

from kronos.common.time import epoch_time_to_kronos_time
from kronos.conf.constants import TIMESTAMP_FIELD
from pykronos import KronosClient

kronos = KronosClient('http://localhost:9191/', blocking=True)

def timeit(desc, func, *args, **kwargs):
  start = time.time()
  func(*args, **kwargs)
  print '  - %s took %ss' % (desc, time.time() - start)

def _make_event(t):
  return {TIMESTAMP_FIELD: epoch_time_to_kronos_time(t),
          'property1': str(uuid.uuid4()),
          'property2': str(uuid.uuid4()),
          'property3': str(uuid.uuid4())}

def insert(stream, n, chunk_size=10000):
  for _ in xrange(0, n, chunk_size):
    events = []
    for t in xrange(0, chunk_size):
      events.append(_make_event(t % 1000))
    kronos.put({stream: events})
    
def read(stream, n, start=0, end=1000):
  for _ in kronos.get(stream,
                      epoch_time_to_kronos_time(start),
                      epoch_time_to_kronos_time(end),
                      limit=n):
    pass
