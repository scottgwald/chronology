from __future__ import absolute_import

import json
import logging
import sys
import time

from pykronos.client import KronosClient
from pykronos.common.time import kronos_time_now

log = logging.getLogger(__name__)


class KronosLoggingMiddleware(object):
  FORWARDED_IP_FIELDS = {'HTTP_X_FORWARDED_FOR',
                         'HTTP_X_FORWARDED_HOST',
                         'HTTP_X_FORWARDED_SERVER'}
  
  def __init__(self):
    from django.conf import settings
    from django.core.exceptions import ImproperlyConfigured
    
    if not hasattr(settings, 'KRONOS_MIDDLEWARE'):
      raise ImproperlyConfigured
    kronos_config = settings.KRONOS_MIDDLEWARE
    if not isinstance(kronos_config, dict):
      raise ImproperlyConfigured
    if 'host' not in kronos_config:
      raise ImproperlyConfigured
    if 'stream' not in kronos_config:
      raise ImproperlyConfigured
    self.client = KronosClient(
      kronos_config['host'],
      blocking=kronos_config.get('blocking', False),
      sleep_block=kronos_config.get('sleep_block', 0.1))
    self.stream = kronos_config['stream']
    self.namespace = kronos_config.get('namespace')
    self.log_exception_stack_trace = kronos_config.get(
        'log_exception_stack_trace', False)
    self.fail_silently = kronos_config.get('fail_silently', False)

  def _get_ip(self, request):
    if not KronosLoggingMiddleware.FORWARDED_IP_FIELDS & set(request.META):
      return request.META.get('REMOTE_ADDR', '').strip()
    for field in KronosLoggingMiddleware.FORWARDED_IP_FIELDS:
      if field in request.META:
        return request.META[field].split(',')[-1].strip()
  
  def process_request(self, request):
    request._kronos_event = {
      'start_time': time.time(),
      '@time': kronos_time_now(),
      'method': request.method.upper(),
      'path': request.path,
      'client_ip': self._get_ip(request)
      }
    get_dict = request.GET.dict()
    if get_dict:
      request._kronos_event['get_params'] = get_dict
    try:
      if not request.body:
        return
    except:
      # The following exception is thrown if the body could not be read.
      # Exception("You cannot access body after reading from request's data
      #           stream")
      return
    post_dict = request.POST.dict()
    if (len(post_dict) == 1 and
        post_dict.values()[0] == '' and
        post_dict.keys()[0] == request.body):
      # There is no real *form* POST.
      try:
        request._kronos_event['body'] = json.loads(request.body)
      except ValueError:
        request._kronos_event['body'] = request.body
    else:
      request._kronos_event['post_params'] = post_dict

  def process_exception(self, request, exception):
    self.client._log_exception(
      request._kronos_event, exception,
      sys.exc_info()[2] if self.log_exception_stack_trace else None)

  def process_response(self, request, response):
    start_time = request._kronos_event.pop('start_time')
    request._kronos_event['duration'] = time.time() - start_time
    try:
      self.client.put({self.stream: [request._kronos_event]},
                      namespace=self.namespace)
    except:
      if self.fail_silently:
        log.error('Failed to log event to Kronos.', exc_info=True)
      else:
        raise
    
    return response
