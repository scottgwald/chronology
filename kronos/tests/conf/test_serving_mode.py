import json
import os

from tests.server import KronosServerTestCase

class TestServingMode(KronosServerTestCase):

  def _hit_endpoint(self, endpoint, data, expected_codes):
    if '/index' in endpoint:
      response = self.http_client.get(path=endpoint)
    else:
      response = self.http_client.post(path=endpoint,
                                       data=json.dumps(data),
                                       buffered=True)
    self.assertTrue(response.status_code in expected_codes)


  def test_endpoints(self):
    mode = os.environ['KRONOS_CONFIG']
    if mode == 'serving_mode_all':
      found = (self.put_path, self.get_path, self.index_path,
               self.delete_path, self.streams_path)
      notfound = ()
    elif mode == 'serving_mode_readonly':
      found = (self.get_path, self.index_path, self.streams_path)
      notfound = (self.put_path, self.delete_path)
    elif mode == 'serving_mode_collector':
      found = (self.put_path, self.index_path)
      notfound = (self.get_path, self.delete_path, self.streams_path)

    for path in found:
      self._hit_endpoint(path, {}, {200, 400})
    for path in notfound:
      self._hit_endpoint(path, {}, {404})
