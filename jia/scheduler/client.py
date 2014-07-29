from __future__ import absolute_import

import urllib
import urllib2
import json
from cStringIO import StringIO
from werkzeug import Request
from scheduler.auth import create_token
from scheduler import app

default_key = app.config['SECRET_KEY']
default_url = "http://%s:%s" % (app.config['SCHEDULER_HOST'],
                                app.config['SCHEDULER_PORT'])


def _send_with_auth(values, secret_key, url):
  """Send dictionary of JSON serializable `values` as a POST body to `url`
     along with `auth_token` that's generated from `secret_key` and `values`

  scheduler.auth.create_token expects a JSON serializable payload, so we send
  a dictionary. On the receiving end of the POST request, the Flask view will
  have access to a werkzeug.datastructures.ImmutableMultiDict. The easiest
  and most surefire way to ensure that the payload sent to create_token will
  be consistent on both ends is to generate an ImmutableMultiDict using the
  werkzeug.Request.
  """

  data = urllib.urlencode(values)

  # Simulate a Flask request because that is what will be unpacked when the
  # request is received on the other side
  request = Request.from_values(content_length=len(data),
    input_stream=StringIO(data),
    content_type='application/x-www-form-urlencoded',
    method='POST')

  # Add the auth_token, re-encode, and send
  values['auth_token'] = create_token(secret_key, dict(request.form))
  data = urllib.urlencode(values)
  req = urllib2.Request(url, data)
  response = urllib2.urlopen(req)
  return json.loads(response.read())


def schedule(code, interval, secret_key=default_key, url=default_url):
  """Schedule a string of `code` to be executed every `interval`

  Specificying an `interval` of 0 indicates the event should only be run
  one time and will not be rescheduled.
  """
  url = '%s/schedule' % url
  values = {
    'interval': interval,
    'code': code,
  }
  return _send_with_auth(values, secret_key, url)


def cancel(task_id, secret_key=default_key, url=default_url):
  """Cancel scheduled task with `task_id`"""
  url = '%s/cancel' % url
  values = {
    'id': task_id,
  }
  return _send_with_auth(values, secret_key, url)
