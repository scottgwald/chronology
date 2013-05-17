# Use cjson instead of simplejson because it's significantly faster. 
# http://j2labs.tumblr.com/post/4262756632/speed-tests-for-json-and-cpickle-in-python
import cjson
import gevent
import gevent.pool
import os
import time
import types

from collections import defaultdict
from functools import wraps
from gunicorn.app.base import Application

import kronos

from kronos.conf import settings
from kronos.core.validators import validate_event, validate_stream
from kronos.core.exceptions import InvalidStreamName
from kronos.storage import router

GREENLET_POOL = gevent.pool.Pool(size=settings.node['greenlet_pool_size'])

def endpoint(methods=['GET']):
  def decorator(function, methods=methods):
    @wraps(function)
    def wrapper(environment, start_response):
      try:
        request_method = environment['REQUEST_METHOD']
        if request_method not in methods:
          start_response('405 Method Not Allowed',
                         [('Allow', ', '.join(methods)),
                          ('Content-Type', 'application/json')])
          return cjson.encode({'@errors': ['%s method not allowed.' %
                                        request_method]})
        # All POST request bodies must be a valid JSON object.
        if request_method == 'POST':
          environment['json'] = cjson.decode(environment['wsgi.input'].read())
        return function(environment, start_response)
      except Exception, e:
        start_response('400 Bad Request',
                       [('Content-Type', 'application/json')])
        return cjson.encode({'@errors': [repr(e)]})

    return wrapper
  # Hack to allow using @endpoint instead of @endpoint() which looks fuuglyyy.
  if type(methods) in (types.MethodType, types.FunctionType):
    return decorator(methods, ['GET'])  
  return decorator

@endpoint
def index(environment, start_response):
  status = {
    'service': 'kronosd',
    'version': kronos.get_version(),
    'id': settings.node.get('id'),
    'fields': settings.stream['fields'],
    'storage': {}
    }
  # Get `is_alive` status from all configured backends.
  for name, backend in router.get_backends():
    status['storage'][name] = {
      'ok': backend.is_alive(),
      'backend': settings.storage[name]['backend']
      }
  start_response('200 OK', [('Content-Type', 'application/json')])
  return cjson.encode(status)

@endpoint(['POST'])
def put_events(environment, start_response):
  start_time = time.time()
  request_json = environment['json']
  errors = []
  events_to_insert = defaultdict(list)

  # Validate stream names.
  streams = request_json.keys()
  for stream in streams:
    try:
      validate_stream(stream)
    except Exception, e:
      errors.append(repr(e))
      # If stream name is invalid, drop all events for that stream in the
      # request.
      del request_json[stream]

  # Validate events.
  for stream in request_json:
    for event in request_json[stream]:
      try:
        validate_event(event)
        events_to_insert[stream].append(event)
      except Exception, e:
        errors.append(repr(e))

  # Spawn greenlets to insert events asynchronously into matching backends.
  greenlets = []
  for stream, events in events_to_insert.iteritems():
    try:
      backends_to_insert = router.backends_to_insert(stream).iteritems()
    except InvalidStreamName, e:
      errors.append(e)
      continue
    for backend, configuration in backends_to_insert:
      greenlets.append(GREENLET_POOL.spawn(backend.insert,
                                           stream, events, configuration))

  # TODO(usmanm): Add async option to API and bypass this wait in that case?
  # Wait for all backends to finish inserting.
  gevent.joinall(greenlets)

  # Did any greenlet fail?
  for greenlet in greenlets:
    if not greenlet.successful():
      errors.append(repr(greenlet.exception))

  # Return count of valid events per stream.
  response = {stream: len(events) for stream, events in
              events_to_insert.iteritems()}
  response['@took'] = '%sms' % int(1000 * (time.time() - start_time))
  if errors:
    response['@errors'] = errors
  start_response('200 OK', [('Content-Type', 'application/json')]) 
  return cjson.encode(response)

# TODO(usmanm): gzip compress response stream?
@endpoint(['POST'])
def get_events(environment, start_response):
  request_json = environment['json']
  backend, configuration = router.backend_to_retrieve(request_json['stream'])
  events_from_backend = backend.retrieve(request_json['stream'],
                                         request_json.get('start_time'),
                                         request_json['end_time'],
                                         request_json.get('start_id'),
                                         configuration)
  start_response('200 OK', [('Content-Type', 'application/json')])
  for event in events_from_backend:
    yield '%s\r\n' % cjson.encode(event)


# Map URLs to handler functions.
urls = {
  '/1.0/index': index,
  '/1.0/events/put': put_events,
  '/1.0/events/get': get_events
  }

def wsgi_application(environment, start_response):
  path = environment.get('PATH_INFO', '').rstrip('/')
  if path in urls:
    return urls[path](environment, start_response)
  else:
    start_response('404 NOT FOUND', [('Content-Type', 'text/plain')])
    return "Four, oh, four :'(."

class GunicornApplication(Application):
  def __init__(self, options={}):
    # Try creating log directory, if missing.
    if not os.path.exists(settings.node['log_directory']):
      os.makedirs(settings.node['log_directory'])
    self.options = {
      'worker_class': 'gevent',
      'accesslog': '%s/%s' % (settings.node['log_directory'].rstrip('/'),
                              'access.log'),
      'errorlog': '%s/%s' % (settings.node['log_directory'].rstrip('/'),
                             'error.log'),
      'log_level': 'info',
      'name': 'kronosd'
      }
    self.options.update(options)
    super(GunicornApplication, self).__init__()

  def init(self, *args):
    config = {}
    for key, value in self.options.iteritems():
      if key in self.cfg.settings and value is not None:
        config[key] = value
    return config
  
  def load(self):
    return wsgi_application
