import json
import sys
import time

from collections import defaultdict

import kronos

# Validate settings before importing anything else.
from kronos.core.validators import validate_settings
from kronos.conf import settings; validate_settings(settings)

from kronos.common.concurrent import GreenletExecutor
from kronos.conf.constants import ResultOrder
from kronos.core.validators import validate_event
from kronos.core.validators import validate_stream
from kronos.storage import router
from kronos.utils.decorators import endpoint
from kronos.utils.decorators import ENDPOINTS

async_executor = GreenletExecutor(
  num_greenlets=settings.node.greenlet_pool_size)

  
@endpoint('/1.0/index')
def index(environment, start_response, headers):
  """
  Return the status of this Kronos instance + its backends>
  Doesn't expect any URL parameters.
  """

  start_time = time.time()
  response = {'service': 'kronosd',
              'version': kronos.__version__,
              'id': settings.node['id'],
              'storage': {}}

  # Check if each backend is alive
  for name, backend in router.get_backends():
    response['storage'][name] = {'alive': backend.is_alive(),
                                 'backend': settings.storage[name]['backend']}
  response['@took'] = '{0}ms'.format(1000 * (time.time() - start_time))
  
  start_response('200 OK', headers)
  return json.dumps(response)

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

  start_time = time.time()
  errors = []
  events_to_insert = defaultdict(list)
  request_json = environment['json']
  namespace = request_json.get('namespace', settings.default_namespace)

  # Validate streams and events
  for stream, events in request_json.get('events', {}).iteritems():
    try:
      validate_stream(stream)
    except Exception, e:
      errors.append(repr(e))
      continue

    for event in events:
      try:
        validate_event(event)
        events_to_insert[stream].append(event)
      except Exception, e:
        errors.append(repr(e))

  results = {}
  for stream, events in events_to_insert.iteritems():
    backends = router.backends_to_mutate(namespace, stream)
    for backend, configuration in backends.iteritems():
      results[(stream, backend.name)] = async_executor.submit(
        backend.insert,
        [namespace, stream, events, configuration])

  # TODO(usmanm): Add async option to API and bypass this wait in that case?
  # Wait for all backends to finish inserting.
  async_executor.wait(results.values())

  # Did any insertion fail?
  response = defaultdict(dict)
  for (stream, backend), result in results.iteritems():
    try:
      result.get()
      response[stream][backend] = len(events_to_insert[stream])
    except Exception, e:
      response[stream][backend] = -1
      errors.append(repr(e))

  # Return count of valid events per stream.
  response['@took'] = '{0}ms'.format(1000 * (time.time() - start_time))
  if errors:
    response['@errors'] = errors

  start_response('200 OK', headers)
  return json.dumps(response)


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
    validate_stream(request_json['stream'])
  except Exception, e:
    start_response('400 Bad Request', headers)
    yield json.dumps({'@errors' : [repr(e)]})
    return

  namespace = request_json.get('namespace', settings.default_namespace)
  limit = int(request_json.get('limit', sys.maxint))
  if limit <= 0:
    events_from_backend = []
  else:
    backend, configuration = router.backend_to_retrieve(namespace,
                                                        request_json['stream'])
    events_from_backend = backend.retrieve(
        namespace,
        request_json['stream'],
        long(request_json.get('start_time', 0)),
        long(request_json['end_time']),
        request_json.get('start_id'),
        configuration,
        order=request_json.get('order', ResultOrder.ASCENDING),
        limit=limit)
  
  start_response('200 OK', headers)
  for event in events_from_backend:
    # TODO(usmanm): Once all backends start respecting limit, remove this check.
    if limit <= 0:
      break
    yield '{0}\r\n'.format(json.dumps(event))
    limit -= 1
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

  start_time = time.time()
  request_json = environment['json']
  try:
    validate_stream(request_json['stream'])
  except Exception, e:
    start_response('400 Bad Request', headers)
    return json.dumps({'@errors' : [repr(e)]})

  namespace = request_json.get('namespace', settings.default_namespace)
  backends = router.backends_to_mutate(namespace, request_json['stream'])
  statuses = {}
  for backend, conf in backends.iteritems():
    statuses[backend.name] = async_executor.submit(
      backend.delete,
      [namespace,
       request_json['stream'],
       long(request_json.get('start_time', 0)),
       long(request_json['end_time']),
       request_json.get('start_id'),
       conf])

  async_executor.wait(statuses.values())

  errors = []
  response = {}
  for backend, status in statuses.iteritems():
    try:
      response[backend] = status.get()
    except Exception, e:
      errors.append(repr(e))
      response[backend] = -1

  response = {request_json['stream']: response}
  response['@took'] = '{0}ms'.format(1000 * (time.time() - start_time))
  if errors:
    statuses['@errors'] = errors
  
  start_response('200 OK', headers)
  return json.dumps(response)


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
        yield '{0}\r\n'.format(json.dumps(stream))
  yield ''


def application(environment, start_response):
  path = environment.get('PATH_INFO', '').rstrip('/')
  if path in ENDPOINTS:
    return ENDPOINTS[path](environment, start_response)
  else:
    start_response('404 NOT FOUND', [('Content-Type', 'text/plain')])
    return "Four, oh, four :'("
