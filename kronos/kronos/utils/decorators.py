import logging
import time
import types

from functools import wraps

from kronos.common.decorators import profile
from kronos.conf import settings
from kronos.conf.constants import ERRORS_FIELD
from kronos.conf.constants import ServingMode
from kronos.conf.constants import SUCCESS_FIELD
from kronos.conf.constants import TOOK_FIELD
from kronos.core import marshal

log = logging.getLogger(__name__)

# Map paths to the functions that serve them
ENDPOINTS = {}

# Which endpoints should we allow access to inside the endpoint
# decorator below for the various serving modes?
_serving_mode_endpoints = {
  ServingMode.ALL: frozenset({'index', 'put_events', 'get_events',
                              'delete_events', 'list_streams'}),
  ServingMode.READONLY: frozenset({'index', 'get_events', 'list_streams'}),
  ServingMode.COLLECTOR: frozenset({'index', 'put_events'}),
  }


def is_remote_allowed(remote):
  """
  Check if `remote` is allowed to make a CORS request.
  """
  if settings.debug:
    return True
  if not remote:
    return False
  for domain_pattern in settings.node['cors_whitelist_domains']:
    if domain_pattern.match(remote):
      return True
  return False


def endpoint(url, methods=['GET']):
  """
  Returns a decorator which when applied a function, causes that function to
  serve `url` and only allows the HTTP methods in `methods`
  """
  def decorator(function, methods=methods):
    # Always allow OPTIONS since CORS requests will need it.
    methods = set(methods)
    methods.add('OPTIONS')
    
    @wraps(function)
    def wrapper(environment, start_response):
      try:
        start_time = time.time()

        if function.func_name not in (_serving_mode_endpoints
                                      [settings.serving_mode]):
          start_response('403 Forbidden',
                         [('Content-Type', 'application/json')])
          return marshal.dumps({
            ERRORS_FIELD: ['kronosd is configured to block access to this '
                           'endpoint.'],
            SUCCESS_FIELD: False,
            TOOK_FIELD: '%fms' % (1000 * (time.time() - start_time))
            })
        req_method = environment['REQUEST_METHOD']

        # If the request method is not allowed, return 405.
        if req_method not in methods:
          start_response('405 Method Not Allowed',
                         [('Allow', ', '.join(methods)),
                          ('Content-Type', 'application/json')])
          return marshal.dumps({
            ERRORS_FIELD: ['%s method not allowed' % req_method],
            SUCCESS_FIELD: False,
            TOOK_FIELD: '%fms' % (1000 * (time.time() - start_time))
            })

        headers = []
        remote_origin = environment.get('HTTP_ORIGIN')
        
        if req_method == 'OPTIONS':
          # This is a CORS preflight request so check that the remote domain is
          # allowed and respond with appropriate CORS headers.
          # http://www.html5rocks.com/static/images/cors_server_flowchart.png
          if is_remote_allowed(remote_origin):
            headers.extend([
              ('Access-Control-Allow-Origin', remote_origin),
              ('Access-Control-Allow-Methods', ', '.join(methods)),
              ('Access-Control-Allow-Headers', ', '.join(
                ('Accept', 'Content-Type', 'Origin', 'X-Requested-With')))
              ])
          # We just tell the client that CORS is ok. Client will follow up
          # with another request to get the answer.
          start_response('200 OK', headers)
          return ''

        # All POST bodies must be json, so decode it here.
        if req_method == 'POST':
          try:
            environment['json'] = marshal.loads(environment['wsgi.input'].read())
          except ValueError:
            start_response('400 Bad Request',
                           [('Content-Type', 'application/json')])
            return marshal.dumps({
              ERRORS_FIELD: ['Request body must be valid JSON.'],
              SUCCESS_FIELD: False,
              TOOK_FIELD: '%fms' % (1000 * (time.time() - start_time))
              })
        
        # All responses are JSON.
        headers.append(('Content-Type', 'application/json'))

        if remote_origin:
          headers.append(('Access-Control-Allow-Origin', remote_origin))

        response = function(environment, start_response, headers)
        if not isinstance(response, types.GeneratorType):
          response[TOOK_FIELD] = '%fms' % (1000 * (time.time() - start_time))
          response = marshal.dumps(response)
        print response, len(response)
        print [ord(c) for c in response.encode('utf-8', 'backslashreplace')]
        return response
      except Exception, e:
        log.exception('endpoint: uncaught exception!')
        start_response('400 Bad Request',
                       [('Content-Type', 'application/json')])
        return marshal.dumps({
          ERRORS_FIELD: [repr(e)],
          SUCCESS_FIELD: False,
          TOOK_FIELD: '%fms' % (1000 * (time.time() - start_time))
          })

    if settings.profile:
      wrapper = profile(wrapper)

    # Map the URL to serve to this function. Only map certain
    # endpoints if serving_mode is restrictive.
    global ENDPOINTS
    ENDPOINTS[url] = wrapper

    return wrapper

  return decorator
