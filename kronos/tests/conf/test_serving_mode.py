import json
import os

from kronos.conf.constants import ServingMode
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
    from kronos.conf import settings

    self.assertTrue(settings.serving_mode in os.environ['KRONOS_CONFIG'])

    if settings.serving_mode == ServingMode.ALL:
      found = (self.put_path, self.get_path, self.index_path,
               self.delete_path, self.streams_path)
      not_found = ()
    elif settings.serving_mode == ServingMode.READONLY:
      found = (self.get_path, self.index_path, self.streams_path)
      not_found = (self.put_path, self.delete_path)
    elif settings.serving_mode == ServingMode.COLLECTOR:
      found = (self.put_path, self.index_path)
      not_found = (self.get_path, self.delete_path, self.streams_path)

    for path in found:
      self._hit_endpoint(path, {}, {200, 400})
    for path in not_found:
      self._hit_endpoint(path, {}, {404})
