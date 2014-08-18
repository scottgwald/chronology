import sys
import os

# uWSGI is started by root, but this process runs as the kronos user.
# This casuses permissions issues on the python egg cache, so we need to
# create our own to avoid that.
try:
  os.mkdir('/tmp/kronos_egg_cache')
except OSError as e:
  if e.errno != 17:
    raise e
os.environ['PYTHON_EGG_CACHE'] = '/tmp/kronos_egg_cache'

# If running as service, we must call all these patch functions.
if 'gevent.monkey' not in sys.modules:
  import gevent.monkey; gevent.monkey.patch_all()

import logging

from collections import defaultdict
from cStringIO import StringIO

import kronos

# Validate settings before importing anything else.
from kronos.core.validator import validate_settings
from kronos.conf import settings; validate_settings(settings)
from kronos.conf import logging as klogging; klogging.configure()

from kronos.conf.constants import ERRORS_FIELD
from kronos.conf.constants import MAX_LIMIT
from kronos.conf.constants import ResultOrder
from kronos.conf.constants import SUCCESS_FIELD
from kronos.core import marshal
from kronos.core.executor import execute_greenlet_async
from kronos.core.executor import wait
from kronos.core.validator import validate_event_and_assign_id
from kronos.core.validator import validate_stream
from kronos.storage.router import router
from kronos.utils.decorators import endpoint
from kronos.utils.decorators import ENDPOINTS

log = logging.getLogger(__name__)

  
@endpoint('/1.0/index')
def index(environment, start_response, headers):
  """
  Return the status of this Kronos instance + its backends>
  Doesn't expect any URL parameters.
  """
  response = {'service': 'kronosd',
              'version': kronos.__version__,
              'id': settings.node['id'],
              'storage': {},
              SUCCESS_FIELD: True}

  # Check if each backend is alive
  for name, backend in router.get_backends():
    response['storage'][name] = {'alive': backend.is_alive(),
                                 'backend': settings.storage[name]['backend']}
  
  start_response('200 OK', headers)
  return response


@endpoint('/1.0/events/put', methods=['POST'])
def put_events(environment, start_response, headers):
  """
  Store events in backends
  POST body should contain a JSON encoded version of:
    { namespace: namespace_name (optional),
      events: { stream_name1 : [event1, event2, ...],
                stream_name2 : [event1, event2, ...],
                ... }
    }
  Where each event is a dictionary of keys and values.
  """
  errors = []
  events_to_insert = defaultdict(list)
  request_json = environment['json']
  namespace = request_json.get('namespace', settings.default_namespace)

  # Validate streams and events
  for stream, events in request_json.get('events', {}).iteritems():
    try:
      validate_stream(stream)
    except Exception, e:
      log.exception('put_events: stream validation failed for `%s`', stream)
      errors.append(repr(e))
      continue

    for event in events:
      try:
        events_to_insert[stream].append(validate_event_and_assign_id(event))
      except Exception, e:
        log.exception('put_events: event validation failed for `%s`', event)
        errors.append(repr(e))

  results = {}
  for stream, events in events_to_insert.iteritems():
    backends = router.backends_to_mutate(namespace, stream)
    for backend, configuration in backends.iteritems():
      results[(stream, backend.name)] = execute_greenlet_async(
        backend.insert, namespace, stream, events, configuration)
  wait(results.values())

  # Did any insertion fail?
  success = True
  response = defaultdict(dict)
  for (stream, backend), result in results.iteritems():
    try:
      result.get()
      response[stream][backend] = {
        'num_inserted': len(events_to_insert[stream])
        }
    except Exception, e:
      log.exception('put_events: insertion to backend `%s` failed.', backend)
      success = False
      response[stream][backend] = {'num_inserted': -1,
                                   ERRORS_FIELD: [repr(e)]}

  response[SUCCESS_FIELD] = success and not errors
  if errors:
    response[ERRORS_FIELD] = errors

  start_response('200 OK', headers)
  return response


# TODO(usmanm): gzip compress response stream?
@endpoint('/1.0/events/get', methods=['POST'])
def get_events(environment, start_response, headers):
  """
  Retrieve events
  POST body should contain a JSON encoded version of:
    { namespace: namespace_name (optional),
      stream : stream_name,
      start_time : starting_time_as_kronos_time,
      end_time : ending_time_as_kronos_time,
      start_id : only_return_events_with_id_greater_than_me,
      limit: optional_maximum_number_of_events,
      order: ResultOrder.ASCENDING or ResultOrder.DESCENDING (default
             ResultOrder.ASCENDING)
    }
  Either start_time or start_id should be specified. If a retrieval breaks
  while returning results, you can send another retrieval request and specify
  start_id as the last id that you saw. Kronos will only return events that
  occurred after the event with that id.
  """
  request_json = environment['json']
  try:
    stream = request_json['stream']
    validate_stream(stream)
  except Exception, e:
    log.exception('get_events: stream validation failed for `%s`',
                  request_json.get('stream'))
    start_response('400 Bad Request', headers)
    yield marshal.dumps({ERRORS_FIELD : [repr(e)],
                         SUCCESS_FIELD: False})
    return

  namespace = request_json.get('namespace', settings.default_namespace)
  limit = int(request_json.get('limit', MAX_LIMIT))
  if limit <= 0:
    events = []
  else:
    backend, configuration = router.backend_to_retrieve(namespace, stream)
    events = backend.retrieve(
      namespace,
      stream,
      long(request_json.get('start_time', 0)),
      long(request_json['end_time']),
      request_json.get('start_id'),
      configuration,
      order=request_json.get('order', ResultOrder.ASCENDING),
      limit=limit)

  start_response('200 OK', headers)

  string_buffer = StringIO()
  for event in events:
    # TODO(usmanm): Once all backends start respecting limit, remove this check.
    if limit <= 0:
      break
    if string_buffer.tell() >= settings.node.flush_size:
      yield string_buffer.getvalue()
      string_buffer.close()
      string_buffer = StringIO()
    string_buffer.write(event)
    string_buffer.write('\r\n')
    limit -= 1
  if string_buffer.tell():
    yield string_buffer.getvalue()
  string_buffer.close()
  yield ''


@endpoint('/1.0/events/delete', methods=['POST'])
def delete_events(environment, start_response, headers):
  """
  Delete events
  POST body should contain a JSON encoded version of:
    { namespace: namespace_name (optional),
      stream : stream_name,
      start_time : starting_time_as_kronos_time,
      end_time : ending_time_as_kronos_time,
      start_id : only_delete_events_with_id_gte_me,
    }
  Either start_time or start_id should be specified. 
  """
  request_json = environment['json']
  try:
    stream = request_json['stream']
    validate_stream(stream)
  except Exception, e:
    log.exception('delete_events: stream validation failed for `%s`.',
              request_json.get('stream'))
    start_response('400 Bad Request', headers)
    return {ERRORS_FIELD : [repr(e)]}

  namespace = request_json.get('namespace', settings.default_namespace)
  backends = router.backends_to_mutate(namespace, stream)
  statuses = {}
  for backend, conf in backends.iteritems():
    statuses[backend.name] = execute_greenlet_async(
      backend.delete,
      namespace,
      stream,
      long(request_json.get('start_time', 0)),
      long(request_json['end_time']),
      request_json.get('start_id'),
      conf)
  wait(statuses.values())

  success = True
  response = {}
  for backend, status in statuses.iteritems():
    try:
      num_deleted, errors = status.get()
      response[backend] = {'num_deleted': num_deleted}
      if errors:
        success = False
        response[ERRORS_FIELD] = errors
    except Exception, e:
      log.exception('delete_events: delete from backend `%s` failed.', backend)
      success = False
      response[backend] = {'num_deleted': -1,
                           ERRORS_FIELD: [repr(e)]}

  response = {request_json['stream']: response,
              SUCCESS_FIELD: success}
  
  start_response('200 OK', headers)
  return response


@endpoint('/1.0/streams', methods=['POST'])
def list_streams(environment, start_response, headers):
  """
  List all streams and their properties that can be read from Kronos right now.
  POST body should contain a JSON encoded version of:
    { namespace: namespace_name (optional)
    }
  """
  start_response('200 OK', headers)
  streams_seen_so_far = set()
  namespace = environment['json'].get('namespace', settings.default_namespace)
  for prefix, backend in router.get_read_backends(namespace):
    for stream in backend.streams(namespace):
      if stream.startswith(prefix) and stream not in streams_seen_so_far:
        streams_seen_so_far.add(stream)
        yield '{0}\r\n'.format(marshal.dumps(stream))
  yield ''


def application(environment, start_response):
  path = environment.get('PATH_INFO', '').rstrip('/')
  if path in ENDPOINTS:
    return ENDPOINTS[path](environment, start_response)
  else:
    start_response('404 NOT FOUND', [('Content-Type', 'text/plain')])
    return "Four, oh, four :'("
