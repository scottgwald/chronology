import datetime
import sys
from jia.utils import get_seconds
from jia.errors import PyCodeError
from pykronos import KronosClient
from pykronos.utils.cache import QueryCache
from pykronos.utils.time import datetime_to_epoch_time
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import epoch_time_to_kronos_time
from pykronos.utils.time import kronos_time_to_datetime
from scheduler import client as scheduler_client
from scheduler import app

import logging; logging.basicConfig()
log = logging.getLogger(__name__)

"""Utilities for executing Jia queries.

Jia panels can enable precomputation. For queries that do significant
computation but reduce the result set size (e.g., aggregation queries) the wait
time for results can be greatly reduced by automatically computing and caching
the results on a regular interval.

This module provides `enable_precompute` and `disable_precompute` methods to
handle the activation and deactivation of precomptue tasks. When precompute is
scheduled for a panel, the user-provided python blob is injected into
`PRECOMPUTE_INITIALIZATION_CODE` and sent over the network to the Jia
scheduler.

Time range queries (with a specified start and end date) are sent to the
scheduler with an interval of 0, meaning they will only be computed once.
Queries with a timeframe mode of 'recent' will be computed repeatedly on a
regular interval.

A `QueryCompute` class (which is used by the `PRECOMPUTE_INITIALIZATION_CODE`)
provides methods for caching the results of a query, as well as running the
query with or without help from the cache.
"""


PRECOMPUTE_INITIALIZATION_CODE = """
from jia.compute import QueryCompute
query = u'''%s'''
timeframe = %s
bucket_width = %s
untrusted_time = %s

compute = QueryCompute(query, timeframe, bucket_width=bucket_width,
                       untrusted_time=untrusted_time)
compute.cache()
"""


DT_FORMAT = '%a %b %d %Y %H:%M:%S'


class QueryCompute(object):
  """A class for executing Jia queries

  Provides `compute` and `cache` methods. `compute(use_cache=False)` can be
  called to simply run the query within a given `timeframe`. Otherwise,
  `bucket_width` must be specified to get from the cache. In order to write to
  the cache via the `cache` method, both `bucket_width` and `untrusted_time`
  must be specified.
  """
  def __init__(self, query, timeframe, bucket_width=None, untrusted_time=None):
    """Initialize QueryCompute
    :param query: A string of python code to execute as a Jia query.
    :param timeframe: A timeframe dictionary. It specifies a mode, which can be
    'recent' or 'range'. Depending on which mode is selected, some of the other
    parameters will be unused. The unused parameters come from the frontend for
    the purposes of storing default/previous values. If the mode is recent,
    only 'value' and 'scale' are used. If the mode is 'range', only 'from' and
    'to' are used.

    Example timeframe:
    timeframe = {
      'mode': 'recent',
      'value': 1,
      'scale': 'days',
      'from': 'Sat Jun 10 2014 00:00:00',
      'to': 'Sun Jun 11 2014 00:00:00',
    }

    :param bucket_width: Optional bucket width in seconds
    :param untrusted_time: Optional untrusted time interval in seconds
    """
    self._query = query
    self._bucket_width = bucket_width
    self._untrusted_time = untrusted_time
    self._start_time, self._end_time = self._get_timeframe_bounds(timeframe,
                                                                  bucket_width)

    self._cache_client = KronosClient(
        app.config['CACHE_KRONOS_URL'],
        namespace=app.config['CACHE_KRONOS_NAMESPACE'],
        blocking=False, sleep_block=0.2)

    # The query is sent through as an unused unique_id argument so that the
    # QueryCache hash can properly uniquely identify it
    unique = {
      'unique_id': self._query
    }

    bucket_width_timedelta = datetime.timedelta(seconds=bucket_width)
    self._query_cache = QueryCache(self._cache_client, self._run_query,
                                   bucket_width_timedelta,
                                   app.config['CACHE_KRONOS_NAMESPACE'],
                                   query_function_kwargs=unique)



  def _get_timeframe_bounds(self, timeframe, bucket_width):
    """
    Get a `bucket_width` aligned `start_time` and `end_time` from a
    `timeframe` dict
    """
    if bucket_width:
      bucket_width_seconds = bucket_width
      bucket_width = epoch_time_to_kronos_time(bucket_width)

    # TODO(derek): Potential optimization by setting the end_time equal to the
    # untrusted_time if end_time > untrusted_time and the results are not being
    # output to the user (only for caching)
    if timeframe['mode'] == 'recent':
      # Set end_time equal to now and align to bucket width
      end_time = datetime_to_kronos_time(datetime.datetime.now())
      original_end_time = end_time
      duration = get_seconds(timeframe['value'], timeframe['scale'])
      duration = epoch_time_to_kronos_time(duration)
      start_time = original_end_time - duration

      if bucket_width:
        # Align values to the bucket width
        # TODO(derek): Warn the user that the timeframe has been altered to fit
        # the bucket width
        if (end_time % bucket_width) != 0:
          end_time += bucket_width - (end_time % bucket_width)

        if (start_time % bucket_width) != 0:
          start_time -= (start_time % bucket_width)

      start = kronos_time_to_datetime(start_time)
      end = kronos_time_to_datetime(end_time)
    elif timeframe['mode'] == 'range':
      end = datetime.datetime.strptime(timeframe['to'], DT_FORMAT)
      end_seconds = datetime_to_epoch_time(end)

      start = datetime.datetime.strptime(timeframe['from'], DT_FORMAT)
      start_seconds = datetime_to_epoch_time(start)

      if bucket_width:
        # Align values to the bucket width
        # TODO(derek): Warn the user that the timeframe has been altered to fit
        # the bucket width
        start_bump = start_seconds % bucket_width_seconds
        start -= datetime.timedelta(seconds=start_bump)
        if (end_seconds % bucket_width_seconds) != 0:
          end_bump = bucket_width_seconds - (end_seconds % bucket_width_seconds)
          end += datetime.timedelta(seconds=end_bump)
    else:
      raise ValueError("Timeframe mode must be 'recent' or 'range'")

    return start, end

  def _run_query(self, start_time, end_time, unique_id=None):
    """Executes a Python query string and returns events

    Acts as a wrapper around exec that injects necessary local variables into the
    scope of the user-provided query blob.

    :param start_time: Python datetime to be injected into query
    :param end_time: Python datetime to be injected into query
    :param unique_id: An unused flag that allows the scheduler to hash this
    function uniquely based on its args when it passes through
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
      exec self._query in {}, locals_dict # No globals.
    except:
      log.error('Error running code', exc_info=True)
      _, exception, tb = sys.exc_info()
      raise PyCodeError(exception, tb)

    events = sorted(locals_dict.get('events', []),
                    key=lambda event: event['@time'])

    return events

  def compute(self, use_cache=True):
    """Call a user defined query and return events with optional help from
    the cache.

    :param use_cache: Specifies whether the cache should be used when possible
    """
    if use_cache:
      if not self._bucket_width:
        raise ValueError('QueryCompute must be initialized with a bucket_width'
                         ' to use caching features.')
      return list(self._query_cache.retrieve_interval(self._start_time,
                                                      self._end_time,
                                                      compute_missing=True))
    else:
      return self._run_query(self._start_time, self._end_time)

  def cache(self):
    """Call a user defined query and cache the results"""
    if not self._bucket_width or self._untrusted_time is None:
      raise ValueError('QueryCompute must be initialized with a bucket_width '
                       'and an untrusted_time in order to write to the cache.')

    now = datetime.datetime.now()
    untrusted_time = now - datetime.timedelta(seconds=self._untrusted_time)
    list(self._query_cache.compute_and_cache_missing_buckets(
        self._start_time,
        self._end_time,
        untrusted_time))


def enable_precompute(panel):
  """Schedule a precompute task for `panel`"""
  query = panel['data_source']['code']
  precompute = panel['data_source']['precompute']
  timeframe = panel['data_source']['timeframe']
  bucket_width_seconds = get_seconds(precompute['bucket_width']['value'],
                                     precompute['bucket_width']['scale'])

  if timeframe['mode'] == 'recent':
    untrusted_time_seconds = get_seconds(precompute['untrusted_time']['value'],
                                         precompute['untrusted_time']['scale'])
    # Schedule the task with an interval equal to the bucket_width
    interval = bucket_width_seconds
  elif timeframe['mode'] == 'range':
    untrusted_time_seconds = 0
    # Schedule the task with an interval of 0 so it only runs once
    interval = 0

  task_code = PRECOMPUTE_INITIALIZATION_CODE % (query, timeframe,
                                                bucket_width_seconds,
                                                untrusted_time_seconds)
  result = scheduler_client.schedule(task_code, interval)

  if result['status'] != 'success':
    raise RuntimeError(result.get('reason'))

  return result['id']


def disable_precompute(panel):
  """Cancel precomputation for `panel`"""
  task_id = panel['data_source']['precompute']['task_id']
  result = scheduler_client.cancel(task_id)
  if result['status'] != 'success':
    raise RuntimeError(result.get('reason'))

