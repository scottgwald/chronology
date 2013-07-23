# TODO(meelap): Moar tests
# - Invalid configuration parameters
# - Concurrent clients
# - Backend specific tests

import json
import os
import random
import re
import sys
import time
import unittest
from werkzeug.test import Client
from werkzeug.wrappers import BaseResponse

import kronos.conf.settings

# Override kronos.conf.settings to whatever we want before importing the Kronos
# application itself.
kronos.conf.settings.storage = {
  'memory': {
    'backend': 'memory.InMemoryStorage',
    'default_max_items': 1000000
  }
}
kronos.conf.settings.node = {
  'id': 'test',
  'greenlet_pool_size': 25,
  'log_directory': 'log',
  'cors_whitelist_domains': map(re.compile, [
    'localhost',
  ])
}
kronos.conf.settings.stream = {
  'fields': {
    'id': '@id',
    'timestamp': '@time'
  },
  'format': re.compile(r'^[a-z0-9\_]+(\.[a-z0-9\_]+)*$', re.I)
}

from kronos.server import wsgi_application

def now():
  return int(time.time())

class KronosServerTest(unittest.TestCase):
  def setUp(self):
    self.kronos_client = Client(wsgi_application, BaseResponse)
    self.get_path = '/1.0/events/get'
    self.put_path = '/1.0/events/put'

  def tearDown(self):
    pass

  def put(self, stream, events):
    data = json.dumps({stream : events})
    resp = self.kronos_client.post(path=self.put_path,
                                   data=data,
                                   buffered=True)
    self.assertEqual(resp.status_code, 200)
    return json.loads(resp.data)

  def get(self, stream, start_time, end_time, start_id=None):
    data = {'stream':stream, 'end_time':end_time }
    if start_id:
      data['start_id'] = start_id
    else:
      data['start_time'] = start_time
    data = json.dumps(data)
    resp = self.kronos_client.post(path=self.get_path,
                                   data=data,
                                   buffered=True)
    return map(json.loads, resp.data.splitlines())

  def test_put_and_get(self):
    stream = "kronos_server_test_{0}".format(random.random())
    event1 = [{'a': 1, 'b': 2, '@time': 1}]
    event2 = [{'a': 3, 'c': 4, '@time': 3}]
    event3 = [{'d': 3, '@time': 3}]

    resp = self.get(stream, 0, 4)
    self.assertEqual(len(resp), 0)

    self.put(stream, event1)
    resp = self.get(stream, 0, 4)
    self.assertEqual(len(resp), 1)
    resp = self.get(stream, 2, 4)
    self.assertEqual(len(resp), 0)

    self.put(stream, event2)
    resp = self.get(stream, 0, 4)
    self.assertEqual(len(resp), 2)
    resp = self.get(stream, 2, 4)
    self.assertEqual(len(resp), 1)

    self.put(stream, event3)
    resp = self.get(stream, 0, 4)
    self.assertEqual(len(resp), 3)
    resp = self.get(stream, 2, 4)
    self.assertEqual(len(resp), 2)

  def test_error_codes(self):
    resp = self.kronos_client.get(path='/1.0/index')
    self.assertEqual(resp.status_code, 200)

    resp = self.kronos_client.get(path='/mmmmcheese')
    self.assertEqual(resp.status_code, 404)

    # Only POSTing json is allowed.
    resp = self.kronos_client.get(path=self.put_path)
    self.assertEqual(resp.status_code, 405)
    resp = self.kronos_client.get(path=self.get_path)
    self.assertEqual(resp.status_code, 405)
    resp = self.kronos_client.post(path=self.get_path, data='im not json')
    self.assertEqual(resp.status_code, 400)
    resp = self.kronos_client.post(path=self.put_path, data='im not json')
    self.assertEqual(resp.status_code, 400)

  def test_stream_names(self):
    # Test that Kronos validates stream names properly.
    data = json.dumps({'stream': '$#@*', 'start_time': 0, 'end_time': 0})
    resp = self.kronos_client.post(path=self.get_path, data=data)
    self.assertEqual(resp.status_code, 400)

  def test_weird_time_ranges(self):
    # Test that Kronos is robust to queries for weird time ranges.
    stream = "kronos_server_test_{0}".format(random.random())

    time1 = now()
    self.assertEqual([], self.get(stream, 0, time1))

    # Start time == end time
    self.assertEqual([], self.get(stream, 0, 0))

    # Start and end time in the future
    self.assertEqual([], self.get(stream, time1+1000, time1+2000))

    # End time < start time
    self.assertEqual([], self.get(stream, time1, time1-1000))

    # Start time < 0 and end time < 0
    self.assertEqual([], self.get(stream, -2000, -1000))



all_streams_to_memory = {
  '*': {
    'backends': {
      'memory': None
    },
    'read_backend': 'memory'
  }
}
all_streams_to_cassandra = {
  '*': {
    'backends': {
      'cassandra_timewidth': None
    },
    'read_backend': 'cassandra_timewidth'
  }
}
stream_configurations = [
  all_streams_to_memory,
  all_streams_to_cassandra
]

if __name__ == "__main__":
  runner = unittest.TextTestRunner()
  if len(sys.argv) > 1:
    test = unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:])
  else:
    test = unittest.defaultTestLoader.loadTestsFromTestCase(KronosServerTest)

  # Run all tests against each backend configuration.
  for streams in stream_configurations:
    kronos.conf.settings.streams_to_backends = streams
    runner.run(test)
