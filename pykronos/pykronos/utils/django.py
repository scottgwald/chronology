import json
import sys
import time

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from client import KronosClient
from utils import kronos_time_now

class KronosLoggingMiddleware(object):
  def __init__(self):
    if not hasattr(settings, 'KRONOS_MIDDLEWARE'):
      raise ImproperlyConfigured
    kronos_config = settings.KRONOS_MIDDLEWARE
    if not isinstance(kronos_config, dict):
      raise ImproperlyConfigured
    if 'host' not in kronos_config:
      raise ImproperlyConfigured
    if 'stream_name' not in kronos_config:
      raise ImproperlyConfigured
    self.client = KronosClient(
      kronos_config['host'],
      blocking=kronos_config.get('blocking', False),
      sleep_block=kronos_config.get('sleep_block', 0.1))
    self.stream_name = kronos_config['stream_name']
    self.namespace = kronos_config.get('namespace')
    self.log_traceback = kronos_config.get('log_traceback', False)
  
  def process_request(self, request):
    request._kronos = {
      'start_time': time.time(),
      '@time': kronos_time_now(),
      'method': request.method,
      'path': request.path,
      'scheme': request.scheme,
      'query_parameters': request.REQUEST.dict(),
      'remote_addr': request.META.get('HTTP_X_FORWARDED_FOR',
                                      request.META['REMOTE_ADDR'])
      }
    if request.body:
      try:
        request._kronos['body'] = json.loads(request.body)
      except:
        request._kronos['body'] = request.body

  def process_exception(self, request, exception):
    self.client._log_exception(
      request._kronos, exception,
      sys.last_traceback if self.log_traceback else None)

  def process_response(self, request, response):
    start_time = request._kronos.pop('start_time')
    request._kronos['duration'] = time.time() - start_time
    self.client.put({self.stream_name: [request._kronos]},
                    namespace=self.namespace)
    return response
