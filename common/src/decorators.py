import cProfile
import logging
import pstats
import types

from cStringIO import StringIO
from functools import wraps

log = logging.getLogger(__name__)


def profile(function, sortby='cumulative', logger=log):
  @wraps(function)
  def wrapper(*args, **kwargs):
    profiler = cProfile.Profile()
    profiler.enable()

    try:
      result = function(*args, **kwargs)
      # If result is a generator, then exhaust it before stopping the profiler.
      # Otherwise we don't really profile the *true* execution time of the
      # function.
      if isinstance(result, types.GeneratorType):
        result = list(result)
      success = True
    except Exception, e:
      result = e
      success = False

    profiler.disable()
    buf = StringIO()
    pstats.Stats(profiler, stream=buf).sort_stats(sortby).print_stats()
    logger.warning(buf.getvalue())

    if success:
      return result
    raise result

  return wrapper
