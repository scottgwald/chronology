from functools import wraps
from gevent import pool

from metis import app

GREENLET_POOL = pool.Pool(size=app.config['NUM_WORKERS'])

def async(func):
  @wraps(func)
  def wrapper(*args, **kwargs):
    if app.debug:
      # Execute function synchronously if in debug mode.
      return func(*args, **kwargs)
    else:
      # Else execute in the background using a greenlet.
      GREENLET_POOL.spawn(func, *args, **kwargs)
  return wrapper
