import logging

from datetime import timedelta
from pykronos.client import TIMESTAMP_FIELD
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import timedelta_to_kronos_time
from pykronos.utils.time import EPOCH

log = logging.getLogger(__name__)

EARLIEST_TIME = datetime_to_kronos_time(EPOCH)

class FilteringDict(object):
  def __init__(self, a_filter):
    self._filter = a_filter
    self._filter_results = {}

  def _caching_filter(self, key):
    if self._filter:
      result = self._filter_results.get(key, None)
      if result is None:
        result = self._filter(key)
        self._filter_results[key] = result
      return result
    else:
      return True

  def get(self, key):
    return EARLIEST_TIME if self._caching_filter(key) else None


def _stream_earliest_action(client, stream, start, end, user_field,
                            fuzzy_time, last_user_action):
  stream_name, event_filter = stream
  events = client.get(stream_name, start, end)
  user_action = {}
  for idx, event in enumerate(events):
    if idx % 10000 == 0:
      log.debug('...processed', idx, 'events')
    if event_filter and not event_filter(event):
      continue
    user, event_time = event[user_field], event[TIMESTAMP_FIELD]
    last_time = last_user_action.get(user)
    if last_time is not None and ((last_time - fuzzy_time) < event_time):
      last_event_time = user_action.get(user, event_time)
      user_action[user] = min(last_event_time, event_time)
  return user_action


def funnel_analyze(client, streams, start, end, user_field,
                   user_filter, fuzzy_time=timedelta(minutes=5)):
  """
  `streams`: an iterable of (stream name, event filter pairs).  The
  funnel is composed from these streams.  The event filter is a
  function that retruns True/False depending on whether an event on
  the stream should be considered (for segmentation, for instance).
  If the event filter is None, all events will be accepted.

  `start`/`end`: the start and end datetimes to analyze.

  `user_field`: the field on each event specifying the user
  identifier.

  `user_filter`: a function that returns True/False depending on
  whether an event from a user should be considered (for segmentation,
  for instance).  If user_filter is None, all users will be accepted.

  `fuzzy_time`: a timedelta representing the time that two events in
  subsequent streams can be out-of-order with one-another.
  """
  assert len(streams) > 1
  last_user_action = FilteringDict(user_filter)
  fuzzy_time = timedelta_to_kronos_time(fuzzy_time)
  stream_sizes = []
  for stream in streams:
    log.debug('Processing stream', stream[0])
    user_action = _stream_earliest_action(client, stream, start, end,
                                          user_field, fuzzy_time,
                                          last_user_action)
    stream_sizes.append(len(user_action))
    last_user_action = user_action
  return stream_sizes
