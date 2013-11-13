import json
import traceback

from functools import wraps

from kronos.conf import settings

# Map paths to the functions that serve them
ENDPOINTS = {}


def is_remote_allowed(remote):
  """
  Check if `remote` is allowed to make a CORS request.
  """
  if settings.debug:
    return True
  if remote is None:
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
    @wraps(function)
    def wrapper(environment, start_response):
      try:
        req_method = environment['REQUEST_METHOD']

        # If the request method is not allowed, return 405.
        # Always allow OPTIONS since any non-simple CORS request will need it.
        if req_method != 'OPTIONS' and req_method not in methods:
          start_response('405 Method Not Allowed',
                         [('Allow', ', '.join(methods)),
                          ('Content-Type', 'application/json')])
          error = '{0} method not allowed'.format(req_method)
          return json.dumps({'@errors' : [error]})

        headers = []
        remote_origin = (environment.get('HTTP_ORIGIN') or
                         environment.get('REMOTE_ADDR'))
        local_origin = '{}://{}'.format(environment['wsgi.url_scheme'],
                                        environment['HTTP_HOST'])
        if remote_origin not in ('127.0.0.1', local_origin):
          # This is a cross domain request, so check that the remote domain is
          # allowed and include CORS headers.
          if is_remote_allowed(remote_origin):
            cors_headers = [
                ('Access-Control-Allow-Origin', remote_origin),
                ('Access-Control-Allow-Methods', ', '.join(methods)),
                ('Access-Control-Allow-Headers', ', '.join(
                  ('Accept', 'Content-Type', 'Origin', 'X-Requested-With')))
                ]
            if req_method == 'OPTIONS':
              # We just tell the client that CORS is ok. Client will follow up
              # with another request to get the answer.
              start_response('200 OK', cors_headers)
              return ''
            else:
              # We return the answer to the request along with CORS headers.
              headers.extend(cors_headers)
          else:
            # Remote domain is not allowed.
            start_response('200 OK', [])
            return ''

        # All POST bodies must be json, so decode it here.
        if req_method == 'POST':
          environment['json'] = json.loads(environment['wsgi.input'].read())

        # All responses are JSON.
        headers.append(('Content-Type', 'application/json'))

        return function(environment, start_response, headers)
      except Exception, e:
        if settings.debug:
          print e
          print traceback.format_exc()
        start_response('400 Bad Request', headers)
        return json.dumps({'@errors' : [repr(e)]})

    # Map the URL to serve to this function. If running in `collector_mode`,
    # then only map the `put_events` function.
    global ENDPOINTS
    if (settings.collector_mode and
        function.func_name not in ('put_events', 'index')):
      continue
    ENDPOINTS[url] = wrapper

    return wrapper

  return decorator
