from functools import wraps

from flask import jsonify

from jia import errors


def json_endpoint(function):
  @wraps(function)
  def wrapper(*args, **kwargs):
    # Will let abort() exceptions bubble up.
    try:
      return jsonify(function(*args, **kwargs))
    except errors.JiaError as e:
      return e.to_response()
  return wrapper
