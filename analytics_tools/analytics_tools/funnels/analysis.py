import logging

from datetime import timedelta
from pykronos.client import TIMESTAMP_FIELD
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import timedelta_to_kronos_time
from pykronos.utils.time import EPOCH

log = logging.getLogger(__name__)

EARLIEST_TIME = datetime_to_kronos_time(EPOCH)

class FilterCache(object):
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


class IdentityDict(object):
  def __getitem__(self, key):
    return key

  def get(self, key):
    return key

def _stream_earliest_action(client, stream, start, end, fuzzy_time,
                            last_user_action, user_id_mappings):
  stream_name, event_filter, user_field = stream
  events = client.get(stream_name, start, end)
  user_action = {}
  for idx, event in enumerate(events):
    if idx % 10000 == 0:
      log.debug('...processed', idx, 'events')
    if event_filter and not event_filter(event):
      continue
    try:
      user = user_id_mappings[user_field].get(event[user_field])
    except:
      log.error('Unable to get field %s on %s from %s', user_field, stream, event)
    last_time = last_user_action.get(user)
    event_time = event[TIMESTAMP_FIELD]
    # If we've seen an action from this user in the last stream, and
    # if they performed an action on the current stream (fuzzily)
    # after their last action, update their current user action time.
    if (user is not None and
        last_time is not None and
        ((last_time - fuzzy_time) < event_time)):
      user_action[user] = min(user_action.get(user, event_time), event_time)
  return user_action


def _load_user_id_mappings(mappings, user_id_mappers, user_ids):
  for mapping, getter in user_id_mappers.iteritems():
    log.debug('...loading mapping for %s', mapping)
    the_dict = mappings[mapping] = {}
    for user_id in user_ids:
      new_id = getter(user_id)
      if new_id:
        the_dict[new_id] = user_id
      

def _sanity_check_args(streams, user_id_mappers):
  assert len(streams) > 1
  first_stream_user_id = streams[0][2]
  required_mappings = {stream[2] for stream in streams} - {first_stream_user_id}
  # Filter down mappings to only the ones we'll need to execute here.
  user_id_mappers = {required: user_id_mappers[required]
                     for required in required_mappings}
  return streams, user_id_mappers


def funnel_analyze(client, streams, start, end, end_first_funnel_step,
                   user_id_mappers, user_filter,
                   fuzzy_time=timedelta(minutes=5)):
  """
  `streams`: a list of (stream name, event filter, user_id field
  name) tuples.  The funnel is composed from these streams.  The event
  filter is a function that returns True/False depending on whether an
  event on the stream should be considered (for segmentation, for
  instance).  If the event filter is None, all events will be
  accepted.  The user_id field name is the name of the field in an
  event that returns the user identifier.

  `start`/`end`: the start and end datetimes to analyze.

  `end_first_funnel_step`: the end time of the first funnel step. You
  sometimes want this to be earlier than the rest of the other steps
  so you can study how a cohort takes certain actions down the line.

  `user_id_mappers`: a dictionary of the form
    {user_id_field: user_id_mapping_function}.
  A user_id_field entry should exist for
  any user_id fieldname of `streams` subsequent to the first stream
  in the funnel.  For example, if `streams` is:
    [(s1, f1, 'userId'), (s2, f2, 'userId'), (s3, f3, 'username')],
  then user_id_mappings should be:
    {'username': function_from_userId_to_username(userId)}

  `user_filter`: a function that returns True/False depending on
  whether an event from a user should be considered (for segmentation,
  for instance).  If user_filter is None, all users will be accepted.

  `fuzzy_time`: a timedelta representing the time that two events in
  subsequent streams can be out-of-order with one-another.
  """
  assert end >= end_first_funnel_step
  streams, user_id_mappers = _sanity_check_args(streams, user_id_mappers)
  last_user_action = FilterCache(user_filter)
  fuzzy_time = timedelta_to_kronos_time(fuzzy_time)
  stream_sizes = []
  user_id_mappings = {}
  for idx, stream in enumerate(streams):
    log.debug('Processing stream', stream[0])
    step_end = end
    if idx == 0:
      user_id_mappings[stream[2]] = IdentityDict()
      step_end = end_first_funnel_step
    user_action = _stream_earliest_action(client, stream, start, step_end,
                                          fuzzy_time, last_user_action,
                                          user_id_mappings)
    stream_sizes.append(len(user_action))
    last_user_action = user_action
    # For the first stream in the funnel, load the mappings to other
    # user_id formats we'll find in subsequent streams.
    if idx == 0:
      log.debug('Loading user_id mappings')
      _load_user_id_mappings(user_id_mappings, user_id_mappers, last_user_action)
      
  return stream_sizes
