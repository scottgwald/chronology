from kronos.conf.constants import ServingMode
from kronos.core import marshal
from tests.server import KronosServerTestCase

class TestServingMode(KronosServerTestCase):

  def _hit_endpoint(self, endpoint, data, expected_codes, method=None):
    if method:
      kwargs = {'path': endpoint}
      if method == 'POST':
        kwargs.update({'data': marshal.dumps(data),
                       'buffered': True})
      response = getattr(self.http_client, method.lower())(**kwargs)
    elif '/index' in endpoint:
      response = self.http_client.get(path=endpoint)
    else:
      response = self.http_client.post(path=endpoint,
                                       data=marshal.dumps(data),
                                       buffered=True)
    self.assertTrue(response.status_code in expected_codes)


  def test_endpoints(self):
    from kronos.conf import settings

    # Check disallowed methods.
    self._hit_endpoint(self.index_path, {}, {405}, 'POST')
    self._hit_endpoint(self.streams_path, {}, {405}, 'GET')

    # Check forbidden resources.
    mode_to_endpoints = {
      ServingMode.ALL: [(self.put_path, self.get_path, self.index_path,
                         self.delete_path, self.streams_path),
                        ()],
      ServingMode.READONLY: [(self.get_path, self.index_path,
                              self.streams_path),
                             (self.put_path, self.delete_path)],
      ServingMode.COLLECTOR: [(self.put_path, self.index_path),
                              (self.get_path, self.delete_path,
                               self.streams_path)]
      }

    for mode, (found, forbidden) in mode_to_endpoints.iteritems():
      settings.serving_mode = mode
      for path in found:
        self._hit_endpoint(path, {}, {200, 400})
      for path in forbidden:
        self._hit_endpoint(path, {}, {403})
