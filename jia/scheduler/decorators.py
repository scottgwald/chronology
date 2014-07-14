from __future__ import absolute_import

import json
import copy
import random
import gevent

from functools import wraps
from flask import request
from scheduler import app
from scheduler.auth import create_token

try:
  # compare_digest is new as of Python 2.7.7, which is too new to depend on
  from hmac import compare_digest as _compare_digest
except ImportError:
  def _compare_digest(a, b):
    """Timing safe-ish compare

    TODO(derek): Replace this with something third party.

    Depends on a PRNG, so this doesn't really secure anything.

    Can take up to 1 second to return.

    Not as good as the real implementation in Python 2.7.7, but perhaps better
    than nothing.
    """

    gevent.sleep(random.random())
    return a == b


def token_protected_endpoint(function):
  """Requires valid auth_token in POST to access

  An auth_token is built by sending a dictionary built from a
  Werkzeug.Request.form to the scheduler.auth.create_token function. 
  """
  @wraps(function)
  def decorated(*args, **kwargs):
    auth_token = request.form.get('auth_token')
    if not auth_token:
      return json.dumps({
        'status': 'fail',
        'reason': 'You must provide an auth_token',
      })


    data = dict(request.form)
    del data['auth_token']
    correct_token = create_token(app.config['SECRET_KEY'], data)

    if _compare_digest(auth_token, correct_token):
      return function(*args, **kwargs)

    else:
      return json.dumps({
        'status': 'fail',
        'reason': 'Incorrect auth_token',
      })

  return decorated
