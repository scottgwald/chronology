import json
import unittest

from werkzeug.test import Client
from werkzeug.wrappers import BaseResponse

from kronos.app import application

VERSION = 1.0
BASE_PATH = '/%s' % VERSION
EVENT_BASE_PATH = '%s/events' % BASE_PATH


class KronosServerTestCase(unittest.TestCase):
  """ Wrapper `TestCase` class which be used by all server tests because it
  provides a clean API to Kronos and performs all necessary clean up logic.
  """

  def setUp(self):
    self.http_client = Client(application, BaseResponse)
    self.get_path = '%s/get' % EVENT_BASE_PATH
    self.put_path = '%s/put' % EVENT_BASE_PATH
    self.delete_path = '%s/delete' % EVENT_BASE_PATH
    self.index_path = '%s/index' % BASE_PATH    
    self.streams_path = '%s/streams' % BASE_PATH
  
  def index(self):
    response = self.http_client.get(path=self.index_path)
    self.assertEqual(response.status_code, 200)
    return json.loads(response.data)

  def put(self, stream_or_mapping, events=None, namespace=None):
    data = {}
    if isinstance(stream_or_mapping, dict):
      data['events'] = stream_or_mapping
    else:
      self.assertTrue(events is not None)
      data['events'] = {stream_or_mapping: events}
    if namespace is not None:
      data['namespace'] = namespace
    response = self.http_client.post(path=self.put_path,
                                     data=json.dumps(data),
                                     buffered=True)
    self.assertEqual(response.status_code, 200)
    return json.loads(response.data)

  def get(self, stream, start_time, end_time, start_id=None, limit=None,
          order=None, namespace=None):
    data = {'stream': stream, 'end_time': end_time}
    
    if start_id:
      data['start_id'] = start_id
    else:
      data['start_time'] = start_time
    if limit is not None:
      data['limit'] = limit
    if order is not None:
      data['order'] = order
    if namespace is not None:
      data['namespace'] = namespace
    response = self.http_client.post(path=self.get_path,
                                     data=json.dumps(data),
                                     buffered=True)
    self.assertEqual(response.status_code, 200)
    return map(json.loads, response.data.splitlines())

  def delete(self, stream, start_time, end_time, start_id=None, namespace=None):
    data = {'stream': stream, 'end_time': end_time}
    if start_id:
      data['start_id'] = start_id
    else:
      data['start_time'] = start_time
    if namespace is not None:
      data['namespace'] = namespace
    response = self.http_client.post(path=self.delete_path,
                                     data=json.dumps(data),
                                     buffered=True)
    self.assertEqual(response.status_code, 200)
    return json.loads(response.data)
    
  def get_streams(self, namespace=None):
    data = {}
    if namespace is not None:
      data['namespace'] = namespace
    response = self.http_client.post(self.streams_path,
                                     data=json.dumps(data),
                                     buffered=True)
    self.assertEqual(response.status_code, 200)
    return map(json.loads, response.data.splitlines())
