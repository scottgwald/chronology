import json
import sys; import os; sys.path.append(os.path.abspath(os.getcwd()))
import time

from kronos.client import KronosClient
from kronos.conf.constants import ResultOrder

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
    self._now = self._kc_blocking.time_now()
    self._acceptable_responses = {key: set(json.dumps(event, sort_keys=True) for event in value)
                                  for key, value in
                                  self._insert_data.iteritems()}
    
  def run(self):
    print 'Testing non-blocking client'
    self._test_put_get(self._kc_nonblocking)
    print 'Testing blocking client'
    self._test_put_get(self._kc_blocking)

  def _test_put_get(self, client):
    start_time = time.time()
    for ctr in xrange(200):
      client.put(self._insert_data)
      self._num_puts += 1
    print 'Pre-flush puts took %f seconds' % (time.time() - start_time)
    client.flush()
    print 'Post-flush puts took %f seconds' % (time.time() - start_time)
        
    start_time = time.time()
    time_range_start = self._now - 20*1e7
    time_range_end = self._now + 1000*1e7    
    for stream, acceptable_payloads in self._acceptable_responses.iteritems():
      response = list(client.get(stream, time_range_start, time_range_end))
      response_asc = list(client.get(stream, time_range_start, time_range_end,
                                     order=ResultOrder.ASCENDING))
      response_desc = list(client.get(stream, time_range_start, time_range_end,
                                              order=ResultOrder.DESCENDING))
      assert len(response) == (self._num_puts * len(acceptable_payloads))
      assert json.dumps(response_asc, sort_keys=True) == json.dumps(response,
                                                                    sort_keys=True)
      response_desc.reverse()
      assert json.dumps(response_asc, sort_keys=True) == json.dumps(
        response_desc, sort_keys=True)
      for event in response:
        time_val = event[client.TIMESTAMP_FIELD]
        assert (time_val >= time_range_start) and (time_val < time_range_end)
        assert client.ID_FIELD in event
        del event[client.ID_FIELD]
        del event[client.TIMESTAMP_FIELD]
        assert json.dumps(event, sort_keys=True) in acceptable_payloads
    print 'Gets took %f seconds' % (time.time() - start_time)
    
if __name__ == "__main__":
  TestClient('http://0.0.0.0:8150').run()
