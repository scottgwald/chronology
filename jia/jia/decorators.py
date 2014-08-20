import json
import sys
import traceback
from functools import wraps
from flask import Response
from jia import errors
from jia.errors import PyCodeError


def json_endpoint(function):
  @wraps(function)
  def wrapper(*args, **kwargs):
    # Will let abort() exceptions bubble up.
    try:
      try:
        # Don't call flask's `jsonify` because it sometimes
        # pretty-prints output, calling indent=2 when dumping json.
        # This causes certain datatypes (e.g., `numpy.int64`) to be
        # implicitly converted when they shouldn't be.
        response = Response(json.dumps(function(*args, **kwargs)),
                            status=200,
                            mimetype='application/json')
        return response
      except PyCodeError:
        raise
      except:
        _, exception, tb = sys.exc_info()
        raise PyCodeError(exception, traceback.format_tb(tb))
    except errors.JiaError as e:
      return e.to_response()
  return wrapper
