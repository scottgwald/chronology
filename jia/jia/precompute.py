import inspect
import sys
import datetime
from scheduler import client as scheduler_client
from scheduler import app
from jia.utils import get_seconds
from pykronos import KronosClient
from jia.errors import PyCodeError

"""
Jia panels can enable precomputation. For queries that require aggregation or
significant amounts of computation, the wait time for results can be greatly
reduced by automatically computing and caching the results on a regular
interval.

This module provides `schedule` and `cancel` methods to handle the activation
and deactivation of precomptue tasks. When precompute is scheduled for a panel,
the user provided python blob is injected into `PRECOMPUTE_INITIALIZATION_CODE`
combined with `precompute_cache` (for cacluating start and end times) and sent
over the network to the Jia scheduler.

Time range queries (with a specified start and end date) are sent to the
scheduler with an interval of 0, meaning they will only be computed once.
Queries with a timeframe mode of 'recent' will be computed repeatedly on a
regular interval.
"""

PRECOMPUTE_INITIALIZATION_CODE = """
from pykronos.utils.time import epoch_time_to_kronos_time
query = u'''%s'''
timeframe = %s
bucket_width = epoch_time_to_kronos_time(%s)
untrusted_time = %s

precompute_cache(query, timeframe, bucket_width, untrusted_time)
"""

DT_FORMAT = '%a %b %d %Y %H:%M:%S'

def run_query(start_time, end_time, query):
  """Executes a Python query string and returns events

  This function exists so that QueryCache has something to call to get events.
  It is basically a wrapper around exec that injects necessary local variables
  into the scope of the user provided query blob.

  :param start_time: Python datetime to be injected into query
  :param end_time: Python datetime to be injected into query
  :param query: String of Python to be executed 
  """
  client = KronosClient(app.config['KRONOS_URL'],
                        namespace=app.config['KRONOS_NAMESPACE'],
                        blocking=False,
                        sleep_block=0.2)
  
  locals_dict = {
    'kronos_client': client,
    'events': [],
    'start_time': start_time,
    'end_time': end_time,
  }

  try:
    exec query in {}, locals_dict # No globals.
  except:
    _, exception, tb = sys.exc_info()
    raise PyCodeError(exception, tb)

  events = sorted(locals_dict.get('events', []),
                  key=lambda event: event['@time'])

  return events


def precompute_cache(query, timeframe, bucket_width, untrusted_time,
                     cache=True):
  print cache
  """Call a user defined query and return the events, optionally caching them

  This function is capable of being stringified and sent across the network to
  be executed by the scheduler.
  
  :param query: A string of python code to execute. Included in the scope are
  start_time, end_time (both in Kronos time), kronos_client, and events list.
  The contents of the events list will be cached in the bucket for the given
  time range.
  :param timeframe: A timeframe dictionary. It specifies a mode, which can be
  'recent' or 'range'. If the mode is recent, only 'value' and 'scale' are
  used. If the mode is 'range', only 'from' and 'to' are used.
  timeframe = {
    'mode': 'recent',
    'value': 1,
    'scale': 'days',
    'from': 'Sat Jun 10 2014 00:00:00',
    'to': 'Sun Jun 11 2014 00:00:00',
  }
  :param bucket_width: Bucket width in Kronos time.
  :param untrusted_time: Untrusted time in seconds.
  :param cache: If True, results will be cached.
  """
  
  # Imports are in here because this function is going to get stringified and
  # sent over the network to the scheduler
  import math
  import sys
  import datetime
  from scheduler import app
  from jia.errors import PyCodeError
  from jia.precompute import run_query
  from jia.precompute import DT_FORMAT
  from jia.utils import get_seconds
  from pykronos import KronosClient
  from pykronos.utils.time import datetime_to_kronos_time
  from pykronos.utils.time import kronos_time_to_epoch_time
  from pykronos.utils.time import kronos_time_to_datetime
  from pykronos.utils.time import epoch_time_to_kronos_time
  from pykronos.utils.time import datetime_to_epoch_time
  from pykronos.utils.cache import QueryCache

  cache_client = KronosClient(app.config['CACHE_KRONOS_URL'],
                              namespace=app.config['CACHE_KRONOS_NAMESPACE'],
                              blocking=False,
                              sleep_block=0.2)

  if timeframe['mode'] == 'recent':
    # Set end_time equal to now and align to bucket width
    end_time = datetime_to_kronos_time(datetime.datetime.now())
    end_time += bucket_width - (end_time % bucket_width)
    
    # Align the duration to bucket width
    # TODO(derek): Warn the user that the timeframe has been altered to fit
    # the bucket width
    duration = get_seconds(timeframe['value'], timeframe['scale'])
    duration = epoch_time_to_kronos_time(duration)
    duration = (math.ceil(duration / float(bucket_width)) + 1) * bucket_width
    
    start_time = end_time - duration

    start = kronos_time_to_datetime(start_time)
    end = kronos_time_to_datetime(end_time)
    
    now = datetime.datetime.now()
    untrusted = now - datetime.timedelta(seconds=untrusted_time) 
  elif timeframe['mode'] == 'range':
    bucket_width_seconds = kronos_time_to_epoch_time(bucket_width)
    end = datetime.datetime.strptime(timeframe['to'], DT_FORMAT)
    end_seconds = datetime_to_epoch_time(end)
    if (end_seconds % bucket_width_seconds) != 0:
      end_bump = bucket_width_seconds - (end_seconds % bucket_width_seconds)
      end += datetime.timedelta(seconds=end_bump)
    start = datetime.datetime.strptime(timeframe['from'], DT_FORMAT)
    start_seconds = datetime_to_epoch_time(start)
    start_bump = start_seconds % bucket_width_seconds
    start -= datetime.timedelta(seconds=start_bump)
    print "foo", start, end
    untrusted = datetime.datetime.now()
  else:
    raise ValueError("Timeframe mode must be 'recent' or 'range'")
  
  bucket_width_seconds = kronos_time_to_epoch_time(bucket_width)
  bucket_width_timedelta = datetime.timedelta(seconds=bucket_width_seconds)
  
  query_cache = QueryCache(cache_client, run_query,
                     bucket_width_timedelta,
                     app.config['CACHE_KRONOS_NAMESPACE'],
                     query_function_args=[query])

  
  return list(query_cache.retrieve_interval(start, end, untrusted, cache=cache))


def schedule(panel):
  query = panel['data_source']['code']
  precompute = panel['data_source']['precompute']
  timeframe = panel['data_source']['timeframe']
  bucket_width_seconds = get_seconds(precompute['bucket_width']['value'],
                                     precompute['bucket_width']['scale'])
  untrusted_time_seconds = get_seconds(precompute['untrusted_time']['value'],
                                       precompute['untrusted_time']['scale'])

  task_code = inspect.getsource(precompute_cache)
  
  if timeframe['mode'] == 'recent':
    task_code += PRECOMPUTE_INITIALIZATION_CODE % (query, timeframe,
                                                   bucket_width_seconds,
                                                   untrusted_time_seconds)
    result = scheduler_client.schedule(task_code, bucket_width_seconds)
  elif timeframe['mode'] == 'range':
    task_code += PRECOMPUTE_INITIALIZATION_CODE % (query, timeframe,
                                                   bucket_width_seconds, 0)
    result = scheduler_client.schedule(task_code, 0)

  if result['status'] != 'success':
    raise RuntimeError(result.get('reason'))
  
  return result['id']


def cancel(panel):
  task_id = panel['data_source']['precompute']['task_id']
  result = scheduler_client.cancel(task_id)
  if result['status'] != 'success':
    raise RuntimeError(result.get('reason'))

