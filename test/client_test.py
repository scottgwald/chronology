import logging
import sys; import os; sys.path.append(os.path.abspath(os.getcwd()))
import time

from kronos.client import KronosClient

class TestClient:
  def __init__(self, hostname):
    self._kc_blocking = KronosClient(hostname)
    self._kc_nonblocking = KronosClient(hostname, blocking=False)
    self._insert_data = {
        'beached':
          [{'az':1}, {'az': 2},
           {'okay': 3, 'put_a_list_on_it': ['cats']}],
        'moar_streams':
          [{'data': {'is': 'nested'}}],
        }
    self._num_puts = 0
    self._now = time.time()
    self._acceptable_responses = {key: set(str(event) for event in value)
                                  for key, value in
                                  self._insert_data.iteritems()}
    
  def run(self):
    print 'Testing non-blocking client'
    self._test_put_get(self._kc_nonblocking)
    print 'Testing blocking client'
    self._test_put_get(self._kc_blocking)

  def _test_put_get(self, client):
    start_time = time.time()
    for ctr in xrange(2000):
      client.put(self._insert_data)
      self._num_puts += 1
    print 'Pre-flush puts took %f seconds' % (time.time() - start_time)
    client.flush()
    print 'Post-flush puts took %f seconds' % (time.time() - start_time)
        
    start_time = time.time()
    for stream, acceptable_payloads in self._acceptable_responses.iteritems():
      response = list(client.get(stream, self._now - 20, self._now + 1000))
      assert len(response) == (self._num_puts * len(acceptable_payloads))
      for event in response:
        time_val = event[client.time_key]
        assert (time_val >= (self._now - 1)) and (time_val < (self._now + 1000))
        assert client.id_key in event
        del event[client.id_key]
        del event[client.time_key]
        assert str(event) in acceptable_payloads
    print 'Gets took %f seconds' % (time.time() - start_time)
    
if __name__ == "__main__":
  TestClient('http://0.0.0.0:8150').run()
