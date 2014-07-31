import copy
import functools
import json
import requests
import sys
import time
import traceback
import types
import ujson

from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from dateutil.parser import parse
from threading import Lock
from threading import Thread

from pykronos.errors import KronosClientError
from pykronos.common.time import datetime_to_kronos_time
from pykronos.common.time import kronos_time_now

# These are constants, do not modify them.
ERRORS_FIELD = '@errors'
ID_FIELD = '@id'
SUCCESS_FIELD = '@success'
TIMESTAMP_FIELD = '@time'

_DEFAULT_CHUNK_SIZE = 131072 # 128k


class ResultOrder(object):
  ASCENDING = 'ascending'
  DESCENDING = 'descending'


def _get_errors(json_dict):
  errors = []
  for key, value in json_dict.iteritems:
    if key == ERRORS_FIELD:
      errors.extend(value)
    if isinstance(value, dict):
      errors.extend(_get_errors(json_dict, errors))
  return errors


class KronosClient(object):
  """
  Initialize a Kronos client that can connect to a server at `http_url`

  Put requests are non-blocking if `blocking`=False.
  If non-blocking, `sleep_block` specifies the frequency of
    a background thread that flushes events to the server.

  `chunk_size` is the number of bytes read at once into memory when fetching
  events. For best performance it should be set equal to the `node.flush_size`
  setting of the Kronos server.  
  """

  def __init__(self, http_url, blocking=True, sleep_block=0.1, namespace=None,
               chunk_size=_DEFAULT_CHUNK_SIZE):
    http_url = http_url.rstrip('/')
    self._put_url = '%s/1.0/events/put' % http_url
    self._get_url = '%s/1.0/events/get' % http_url
    self._delete_url = '%s/1.0/events/delete' % http_url
    self._index_url = '%s/1.0/index' % http_url
    self._streams_url = '%s/1.0/streams' % http_url

    self.namespace = namespace
    self.chunk_size = chunk_size

    self._blocking = blocking
    if not blocking:
      self._sleep_block = sleep_block
      self._setup_nonblocking()

  def _setup_nonblocking(self):
    self._put_queue = []
    self._put_lock = Lock()

    me = self
    class PutThread(Thread):
      def __init__(self):
        Thread.__init__(self)
        self.daemon = True
      def run(self):
        while True:
          me.flush()
          time.sleep(me._sleep_block)
    PutThread().start()

  def flush(self):
    if self._blocking:
      return
    old_queue = None
    with self._put_lock:
      if self._put_queue:
        old_queue = self._put_queue
        self._put_queue = []
    if old_queue:
      namespace_to_events = defaultdict(lambda: defaultdict(list))
      for namespace, event_dict in old_queue:
        for stream_name, events in event_dict.iteritems():
          namespace_to_events[namespace][stream_name].extend(events)
      for namespace, event_dict in namespace_to_events.iteritems():
        self._put(namespace, event_dict)

  def _log_exception(self, event, exception, tb=None):
    exception_dict = event['exception'] = {
      'message': (exception.message if hasattr(exception, 'message')
                  else str(exception))
    }
    if hasattr(exception, '__module__'):
      exception_dict['class'] = '%s.%s' % (exception.__module__,
                                           exception.__class__.__name__)
    else:
      exception_dict['class'] = exception.__class__.__name__
    if tb:
      exception_dict['stack_trace'] = traceback.extract_tb(tb)

  def index(self):
    response_dict = requests.get(self._index_url).json()
    if not response_dict[SUCCESS_FIELD]:
      raise KronosClientError('Encountered errors %s' %
                              _get_errors(response_dict))
    return response_dict

  def put(self, event_dict, namespace=None):
    """
    Sends a dictionary of `event_dict` of the form {stream_name:
    [event, ...], ...}  to the server.
    
    The `blocking` parameter allows the request to block until the
    server responds, and returns some information on the response.
    Here's an example:

    {u'stream_name_1': 3, u'stream_name_2': 1, u'@took': u'1ms'}
      -> put 3 events on stream_name_1
      -> put 1 event on stream_name_2
      -> put took 1ms to complete

    If `blocking` is false and the process running the client ends
    before flushing the pending data to the server, you might lose
    that data.  Calling `flush` will block until all pending data has
    been acknowledged by the server.
    """
    # Copy the input, in case we need to modify it by adding a timestamp.
    event_dict = copy.deepcopy(event_dict)

    # Ensure that all events have a timestamp.
    timestamp = kronos_time_now()
    for events in event_dict.itervalues():
      for event in events:
        if TIMESTAMP_FIELD not in event:
          event[TIMESTAMP_FIELD] = timestamp
        else:
          if isinstance(event[TIMESTAMP_FIELD], types.StringTypes):
            event[TIMESTAMP_FIELD] = parse(event[TIMESTAMP_FIELD])
          if isinstance(event[TIMESTAMP_FIELD], datetime):
            event[TIMESTAMP_FIELD] = datetime_to_kronos_time(
              event[TIMESTAMP_FIELD])

    namespace = namespace or self.namespace
    
    if self._blocking:
      return self._put(namespace, event_dict)
    else:
      with self._put_lock:
        self._put_queue.append((namespace, event_dict))

  def _put(self, namespace, event_dict):
    request_dict = {'events': event_dict}
    if namespace is not None:
      request_dict['namespace'] = namespace
    
    response = requests.post(self._put_url, data=json.dumps(request_dict))
    if response.status_code != requests.codes.ok:
      raise KronosClientError('Received response code %s with errors %s' %
                              (response.status_code,
                               response.json().get(ERRORS_FIELD)))
    response_dict = response.json()
    if not response_dict[SUCCESS_FIELD]:
      raise KronosClientError('Encountered errors %s' %
                              _get_errors(response_dict))
    return response_dict

  def get(self, stream, start_time, end_time, start_id=None, limit=None,
          order=ResultOrder.ASCENDING, namespace=None, timeout=None):
    """
    Queries a stream with name `stream` for all events between
    `start_time` and `end_time`.  An optional `start_id` allows the
    client to restart from a failure, specifying the last ID they
    read.  An optional `limit` limits the maximum number of
    events returned.  An optional `order` requests results in `ASCENDING`
    or `DESCENDING` order.
    """
    if isinstance(start_time, types.StringTypes):
      start_time = parse(start_time)
    if isinstance(end_time, types.StringTypes):
      end_time = parse(end_time)      
    if isinstance(start_time, datetime):
      start_time = datetime_to_kronos_time(start_time)
    if isinstance(end_time, datetime):
      end_time = datetime_to_kronos_time(end_time)
      
    request_dict = {
      'stream': stream,
      'end_time': end_time,
      'order': order,
    }
    if start_id is not None:
      request_dict['start_id'] = start_id
    else:
      request_dict['start_time'] = start_time

    if limit is not None:
      request_dict['limit'] = limit

    namespace = namespace or self.namespace
    if namespace is not None:
      request_dict['namespace'] = namespace
    
    errors = []
    last_id = None
    while True:
      try:
        response = requests.post(self._get_url,
                                 data=json.dumps(request_dict),
                                 stream=True,
                                 timeout=timeout)
        if response.status_code != requests.codes.ok:
          raise KronosClientError('Bad server response code %d' %
                                  response.status_code)
        for line in response.iter_lines(chunk_size=self.chunk_size):
          if line:
            # Python's json adds a lot of overhead when decoding a large
            # number of events; ujson fares better. However ujson won't work
            # on PyPy since it's a C extension.
            event = ujson.loads(line, precise_float=True)
            last_id = event[ID_FIELD]
            yield event
        break
      except Exception, e:
        if isinstance(e, requests.exceptions.Timeout):
          raise KronosClientError('Request timed out.')
        errors.append(e)
        if len(errors) == 10:
          raise KronosClientError(errors)
        if last_id != None:
          request_dict.pop('start_time', None)
          request_dict['start_id'] = last_id
        time.sleep(len(errors) * 0.1)

  def delete(self, stream, start_time, end_time, start_id=None, namespace=None):
    """
    Delete events in the stream with name `stream` that occurred between
    `start_time` and `end_time`.  An optional `start_id` allows the
    client to delete events starting from an ID rather than a timestamp.
    """
    if isinstance(start_time, types.StringTypes):
      start_time = parse(start_time)
    if isinstance(end_time, types.StringTypes):
      end_time = parse(end_time)      
    if isinstance(start_time, datetime):
      start_time = datetime_to_kronos_time(start_time)
    if isinstance(end_time, datetime):
      end_time = datetime_to_kronos_time(end_time)
    request_dict = {
      'stream': stream,
      'end_time': end_time
    }
    if start_id:
      request_dict['start_id'] = start_id
    else:
      request_dict['start_time'] = start_time

    namespace = namespace or self.namespace
    if namespace is not None:
      request_dict['namespace'] = namespace

    response = requests.post(self._delete_url,
                             data=json.dumps(request_dict),
                             stream=True)
    if response.status_code != requests.codes.ok:
      raise KronosClientError('Bad server response code %d' %
                              response.status_code)
    response_dict = response.json()
    if not response_dict[SUCCESS_FIELD]:
      raise KronosClientError('Encountered errors %s' %
                              _get_errors(response_dict))
    return response_dict

  def get_streams(self, namespace=None):
    """
    Queries the Kronos server and fetches a list of streams available to be
    read.
    """
    request_dict = {}
    namespace = namespace or self.namespace    
    if namespace is not None:
      request_dict['namespace'] = namespace
    response = requests.post(self._streams_url,
                             data=json.dumps(request_dict),
                             stream=True)
    if response.status_code != requests.codes.ok:
      raise KronosClientError('Bad server response code %d' %
                              response.status_code)
    for line in response.iter_lines():
      if line:
        yield json.loads(line)

  def log_function(self, stream_name, properties={},
                   log_function_stack_trace=False,
                   log_exception_stack_trace=False,
                   namespace=None):
    """
    Logs each call to the function as an event in the stream with name
    `stream_name`. If `log_stack_trace` is set, it will log the stack trace
    under the `stack_trace` key. `properties` is an optional mapping fron key
    name to some function which expects the same arguments as the function
    `function` being decorated. The event will be populated with keys in
    `properties` mapped to the return values of the
    `properties[key_name](*args, **kwargs)`.
    Usage:

      @kronos_client.log_function('mystreamname',
                                  properties={'a': lambda x, y: x,
                                              'b': lambda x, y: y})
      def myfunction(a, b):
        <some code here>
    """
    namespace = namespace or self.namespace
    def decorator(function):
      @functools.wraps(function)
      def wrapper(*args, **kwargs):
        event = {}
        start_time = time.time()
        if log_function_stack_trace:
          event['stack_trace'] = traceback.extract_stack()
        try:
          return function(*args, **kwargs)
        except Exception as exception:
          self._log_exception(event, exception,
                              (sys.last_traceback if log_exception_stack_trace
                               else None))
          raise exception
        finally:
          event['duration'] = time.time() - start_time
          for key, value_getter in properties.iteritems():
            event[key] = value_getter(*args, **kwargs)
          self.put({stream_name: [event]}, namespace=namespace)
      return wrapper
    return decorator

  @contextmanager
  def log_scope(self, stream_name, properties={}, log_scope_stack_trace=False,
                log_exception_stack_trace=False, namespace=None):
    """
    Identical to `log_function` except that `log_scope` is used to log blocks
    of code. The API is identical except that keys in `properties` are mapped to
    real values rather than getter functions. Usage:

      with kronos_client.log_scope('mystreamname', properties={ 'lol':'cat' },
                                   log_scope_stack_trace=True):
        <some code here>
    """
    start_time = time.time()
    namespace = namespace or self.namespace
    event = properties.copy()
    if log_scope_stack_trace:
      event['stack_trace'] = traceback.extract_stack()
    try:
      yield event
    except Exception, exception:
      self._log_exception(event, exception,
                          (sys.last_traceback if log_exception_stack_trace
                           else None))
    event['duration'] = time.time() - start_time
    self.put({stream_name: [event]}, namespace=namespace)
