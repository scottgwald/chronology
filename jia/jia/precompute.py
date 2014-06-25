import inspect
import sys
import datetime
from scheduler import client as scheduler_client
from scheduler import app
from jia.utils import get_seconds
from pykronos import KronosClient
from jia.errors import PyCodeError

code = """
from pykronos.utils.time import epoch_time_to_kronos_time
query = u'''%s'''
timeframe = %s
bucket_width = epoch_time_to_kronos_time(%s)
untrusted_time = %s

precompute_cache(query, timeframe, bucket_width, untrusted_time)
"""

DT_FORMAT = '%a %b %d %Y %H:%M:%S'

def run_query(start_time, end_time, query):
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


def precompute_cache(query, timeframe, bucket_width, untrusted_time):
  """Call a user defined query and cache the results
  
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
  from pykronos import KronosClient
  from pykronos.utils.time import datetime_to_kronos_time
  from pykronos.utils.time import kronos_time_to_epoch_time
  from pykronos.utils.time import kronos_time_to_datetime
  from pykronos.utils.cache import QueryCache

  cache_client = KronosClient(app.config['CACHE_KRONOS_URL'],
                              namespace=app.config['CACHE_KRONOS_NAMESPACE'],
                              blocking=False,
                              sleep_block=0.2)

  if timeframe['mode'] == 'recent':
    end_time = datetime_to_kronos_time(datetime.datetime.now())
    end_time -= (end_time % bucket_width)
    start_time = end_time - math.ceil(timeframe / bucket_width) * bucket_width
    start = kronos_time_to_datetime(start_time)
    end = kronos_time_to_datetime(end_time)
    now = datetime.datetime.now()
    untrusted = now - datetime.timedelta(seconds=untrusted_time)
    bucket_width_seconds = kronos_time_to_epoch_time(bucket_width)
    bucket_width_timedelta = datetime.timedelta(seconds=bucket_width_seconds)

  elif timeframe['mode'] == 'range':
    start = datetime.datetime.strptime(timeframe['from'], DT_FORMAT)
    end = datetime.datetime.strptime(timeframe['to'], DT_FORMAT)
    bucket_width_timedelta = datetime.timedelta(seconds=1)
    untrusted = datetime.datetime.now()
   
  cache = QueryCache(cache_client, run_query,
                     bucket_width_timedelta, 'locu_computed',
                     query_function_args=[query])

  
  c = list(cache.compute_and_cache(start, end, untrusted))


def schedule(panel):
  query = panel['data_source']['code']
  precompute = panel['data_source']['precompute']
  timeframe = panel['data_source']['timeframe']
  bucket_width_seconds = get_seconds(precompute['bucket_width'])
  untrusted_time_seconds = get_seconds(precompute['untrusted_time'])

  task_code = inspect.getsource(precompute_cache)
  
  if timeframe['mode'] == 'recent':
    task_code += code % (query, timeframe, bucket_width, untrusted_time)
    result = scheduler_client.schedule(task_code, bucket_width)
  elif timeframe['mode'] == 'range':
    task_code += code % (query, timeframe, 0, 0)
    result = scheduler_client.schedule(task_code, 0)

  if result['status'] != 'success':
    raise RuntimeError(result.get('reason'))
  
  return result['id']


def cancel(panel):
  task_id = panel['data_source']['precompute']['task_id']
  result = scheduler_client.cancel(task_id)
  if result['status'] != 'success':
    raise RuntimeError(result.get('reason'))

