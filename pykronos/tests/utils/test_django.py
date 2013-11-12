import json
import os
import time
import unittest

from django.http import HttpRequest
from django.http import QueryDict

from pykronos.client import ID_FIELD
from pykronos.client import TIMESTAMP_FIELD
from pykronos.utils.django import KronosLoggingMiddleware
from pykronos.utils.time import kronos_time_now

class KronosLoggingMiddlewareTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.conf.django_settings'
    os.putenv('DJANGO_SETTINGS_MODULE', 'tests.conf.django_settings')

  def setUp(self):
    from django.conf import settings
    
    self.middleware = KronosLoggingMiddleware()
    self.stream = settings.KRONOS_MIDDLEWARE['stream']
    self.request = HttpRequest()
    self.request.path = '/yo'
    self.request.method = 'POST'
    self.request.META['REMOTE_ADDR'] = '127.0.0.1'
    self.request.GET = QueryDict('hello=world&lol=cat')
    self.request.POST = QueryDict('potayto=potahto&adam=b')
    self.request._body = 'potayto=potahto&adam=b'

  def test_body_parsing(self):
    body = '{"foo": "baz"}'
    self.request._body = body
    self.request.POST = QueryDict(body)
    self.middleware.process_request(self.request)
    self.assertEqual(self.request._kronos_event['body'],
                     json.loads(body))
    self.assertTrue('post_params' not in self.request._kronos_event)

    body = 'bleurgh'
    self.request._body = body
    self.request.POST = QueryDict(body)
    self.middleware.process_request(self.request)
    self.assertEqual(self.request._kronos_event['body'], body)
    self.assertTrue('post_params' not in self.request._kronos_event)

    body = 'lol=cat&hello=world'
    self.request._body = body
    self.request.POST = QueryDict(body)
    self.middleware.process_request(self.request)
    self.assertEqual(self.request._kronos_event['post_params'],
                     {'lol': 'cat', 'hello': 'world'})
    self.assertTrue('body' not in self.request._kronos_event)

  def test_request_flow(self, with_exception=False):
    start_low = kronos_time_now()
    self.middleware.process_request(self.request)
    start_high = kronos_time_now()

    time.sleep(0.2) # To get some deterministic `duration`.

    if with_exception:
      try:
        raise ValueError('I am a dummy error.')
      except ValueError, exception:
        self.middleware.process_exception(self.request, exception)

    self.middleware.process_response(self.request, None)

    event = self.request._kronos_event
    self.assertTrue(event[TIMESTAMP_FIELD] > start_low)
    self.assertTrue(event[TIMESTAMP_FIELD] < start_high)
    self.assertTrue(event['duration'] > 0.2)
    # This should be true, unless you're on a super slow machine?
    self.assertTrue(event['duration'] < 0.3)
    self.assertEqual(event['client_ip'], '127.0.0.1')
    self.assertEqual(event['path'], '/yo')
    self.assertEqual(event['get_params'], {'hello': 'world', 'lol': 'cat'})
    self.assertEqual(event['post_params'], {'potayto': 'potahto', 'adam': 'b'})
    self.assertEqual(event['method'], 'POST')

    if with_exception:
      exception = event['exception']
      self.assertEqual(exception['class'], 'ValueError')
      self.assertEqual(exception['message'], 'I am a dummy error.')
      self.assertEqual(len(exception['traceback']), 1)

    # Ensure that correct event dict was sent to Kronos.
    events = list(self.middleware.client.get(self.stream,
                                             start_low,
                                             start_high))
    self.assertEqual(len(events), 1)
    event = events[0]
    del event[ID_FIELD]
    self.assertEqual(event, self.request._kronos_event)

  def test_request_flow_with_exception(self):
    self.test_request_flow(with_exception=True)
