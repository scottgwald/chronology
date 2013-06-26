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

# map URLs to the functions that serve them
urls = { }

def endpoint(url, methods=['GET']):
  """
  Returns a decorator which when applied a function, causes that function to
  serve `url` and only allows the HTTP methods in `methods`
  """

  def decorator(function, methods = methods):
    @wraps(function)
    def wrapper(environment, start_response):
      try:
        req_method = environment['REQUEST_METHOD']

        if req_method == 'OPTIONS':
          origin = environment['Origin']
          for cors_allowed in settings.node.cors_whitelist_domans:
            if cors_allowed.match(origin):
              # Origin is allowed, so include CORS headers
              headers = [('Access-Control-Allow-Origin', origin),
                         ('Access-Control-Allow-Methods', methods),
                         ('Access-Control-Allow-Headers',
                         ('Accept', 'Content-Type', 'Origin', 'X-Requested-With'))]
              start_response('200 OK', headers)
              return
          # If origin isn't allowed, don't return any CORS headers
          # Client's browser will treat this as an error
          return

        # If the request method is not allowed, return 405
        if req_method not in methods:
          headers = [('Allow', ', '.join(methods)),
                     ('Content-Type', 'application/json')]
          start_repsonse('405 Method Not Allowed', headers)
          error = '{0} method not allowed'.format(req_method)
          return cjson.encode({'@errors' : [error]})

        # All POST bodies must be json, so decode it here
        if req_method == 'POST':
          environment['json'] = cjson.decode(environment['wsgi.input'].read())

        return function(environment, start_response)
      except Exception, e:
        start_response('400 Bad Request', [('Content-Type', 'application/json')])
        return cjson.encode({'@errors' : [repr(e)]})

    # map the URL to serve to this function
    global urls
    urls[url] = wrapper

    return wrapper

  return decorator

@endpoint('/1.0/index')
def index(environment, start_response):
  """
  Return the status of this Kronos instance + it's backends>
  Doesn't expect any URL parameters.
  """

  status = {'service': 'kronosd',
            'version': kronos.get_version(),
            'id': settings.node.get('id'),
            'fields': settings.stream['fields'],
            'storage': {}}

  # Check if each backend is alive
  for name, backend in router.get_backends():
    status['storage'][name] = {'ok': backend.is_alive(),
                               'backend': settings.storage[name]['backend']}

  start_response('200 OK', [('Content-Type', 'application/json')])
  return cjson.encode(status)

@endpoint('/1.0/events/put', ['POST'])
def put(environment, start_response):
  """
  Store events in backends
  POST body should contain a JSON encoded version of:
    { stream_name1 : [event1, event2, ...],
      stream_name2 : [event1, event2, ...],
    ...
    }
  Where each event is a dictionary of keys and values.
  """

  start_time = time.time()
  errors = []
  events_to_insert = defaultdict(list)

  # Validate streams and events
  for stream, events in environment['json'].iteritems():
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

  # Spawn greenlets to insert events asynchronously into matching backends.
  greenlets = []
  for stream, events in events_to_insert.iteritems():
    backends = router.backends_to_insert(stream)
    for backend, conf in backends.iteritems():
      greenlet = GREENLET_POOL.spawn(backend.insert, stream, events, conf)
      greenlets.append(greenlet)

  # TODO(usmanm): Add async option to API and bypass this wait in that case?
  # Wait for all backends to finish inserting.
  gevent.joinall(greenlets)

  # Did any greenlet fail?
  for greenlet in greenlets:
    if not greenlet.successful():
      errors.append(repr(greenlet.exception))

  # Return count of valid events per stream.
  response = { stream : len(events) for stream, events in
                                        events_to_insert.iteritems() }
  response['@took'] = '{0}ms'.format(1000 * (time.time() - start_time))
  if errors:
    response['@errors'] = errors

  start_response('200 OK', [('Content-Type', 'application/json')]) 
  return cjson.encode(response)

# TODO(usmanm): gzip compress response stream?
@endpoint('/1.0/events/get', ['POST'])
def get_events(environment, start_response):
  """
  Retrieve events
  POST body should contain a JSON encoded version of:
    { stream : stream_name,
      start_time : starting_time_as_unix_time,
      end_time : ending_time_as_unix_time,
      start_id : only_return_events_with_id_greater_than_me
    }
  Either start_time or start_id should be specified. If a retrieval breaks
  while returning results, you can send another retrieval request and specify
  start_id as the last id that you saw. Kronos will only return events that
  occurred after the event with that id.
  """

  request_json = environment['json']
  backend, configuration = router.backend_to_retrieve(request_json['stream'])
  events_from_backend = backend.retrieve(request_json['stream'],
                                         request_json.get('start_time'),
                                         request_json['end_time'],
                                         request_json.get('start_id'),
                                         configuration)

  start_response('200 OK', [('Content-Type', 'application/json')])
  for event in events_from_backend:
    yield '{0}\r\n'.format(cjson.encode(event))

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
    log_dir = settings.node['log_directory'].rstrip('/')
    if not os.path.exists(log_dir):
      os.makedirs(log_dir)

    # Default options
    self.options = {'worker_class': 'gevent',
                    'accesslog': '{0}/{1}'.format(log_dir, 'access.log'),
                    'errorlog': '{0}/{1}'.format(log_dir, 'error.log'),
                    'log_level': 'info',
                    'name': 'kronosd'}

    # Apply user specified options
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
