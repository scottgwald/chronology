import logging
from datetime import timedelta

from common.event_tools import get_property
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


class FunnelStep(object):
  """ Data structure for funnel step properties. """
  def __init__(self, stream_name, event_filter=None, user_field='userId',
               output_fields=None, invert=False):
    """
    :param stream_name: Kronos stream name
    :param event_filter: a function that returns True/False depending on whether
      an event on the stream should be considered (for segmentation, for
      instance).  If the event filter is None, all events will be accepted.
    :param user_field: name of the field in an event that returns the user
      identifier. Defaults to 'userId'.
    :param output_fields: fields in event to output.
    :param invert: boolean. If true, include all users from the previous step
      that do NOT appear in the current event stream or are not filtered by
      `event_filter`. Example: If the stream_name is 'clicked_tab' and the
      event_filter is all Essential accounts, if invert is True, users counted
      in this step of the funnel are those from the previous step that are not
      in 'clicked_tab' stream or are not Essential accounts. The first funnel
      step can not have invert=True. If invert=True, an exception is raised.
    """
    self.stream_name = stream_name
    self.event_filter = event_filter
    self.user_field = user_field
    self.output_fields = output_fields
    self.invert = invert
               

class FunnelOutput(object):
  """ Data structure for storing the output of the funnel. """
  def __init__(self):
    self.step_output = []

  def add(self, step):
    self.step_output.append(step)

  def stream_sizes(self):
    """ Returns size of each funnel step in a list. """
    return [len(s['user_action']) for s in self.step_output]

  def user_ids(self):
    """ Returns list of user ids at each funnel in a list. """
    return [s['user_action'].keys() for s in self.step_output]

  def stream_data(self):
    """ Returns output data of each funnel step in a list.
    Each list element is a dictionary with user id as the key. The value for
    each key is a dictionary with the requested output properties for that
    funnel step as the keys, the values of the properties as the values.
    """
    return [s['stream_data'] for s in self.step_output]


def _stream_earliest_action(client, stream, start, end, fuzzy_time,
                            last_user_action, user_id_mappings):
  """ Find users who advance to this step of the funnel.
  :returns: dictionary with user_action and stream_data. user_action is a
  dictionary of user ids and time of last action. This is for determining
  if events in subsequent streams occur after the current stream.
  stream_data is a dictionary of user ids and dictionary of output properties
  as specified in stream.output_fields.
  """
  events = client.get(stream.stream_name, start, end)
  user_action = {}
  stream_data = {}
  for idx, event in enumerate(events):
    if idx % 10000 == 0:
      log.debug('...processed', idx, 'events')
    if stream.event_filter and not stream.event_filter(event):
      continue
    try:
      user = user_id_mappings[stream.user_field].get(event[stream.user_field])
    except:
      log.error('Unable to get field %s on %s from %s',
                stream.user_field, stream.stream_name, event)
    last_time = last_user_action.get(user)
    event_time = event[TIMESTAMP_FIELD]
    # If we've seen an action from this user in the last stream, and
    # if they performed an action on the current stream (fuzzily)
    # after their last action, update their current user action time.
    if (user is not None and
        last_time is not None and
        ((last_time - fuzzy_time) < event_time)):
      user_action[user] = min(user_action.get(user, event_time), event_time)
      if stream.output_fields and not stream.invert:
        event_fields = {}
        for field in stream.output_fields:
          try:
            event_fields[field] = get_property(event, field)
          except KeyError:
            log.warn('Field %s does not appear in stream %s',
                     field, stream.stream_name)
        stream_data[user] = event_fields

  # If stream results should be inverted, include all users that are NOT in
  # user_action, and use their timestamp from the previous step as the timestamp
  # of the current step. We can not use the timestamp for this stream, since they
  # may not have an event in this stream.
  if stream.invert:
    inverted_user_action = {}
    for user, timestamp in last_user_action.iteritems():
      if user not in user_action:
        inverted_user_action[user] = timestamp
    return {'user_action': inverted_user_action, 'stream_data': {}}
  else:        
    return {'user_action': user_action, 'stream_data': stream_data}


def _load_user_id_mappings(mappings, user_id_mappers, user_ids):
  for mapping, getter in user_id_mappers.iteritems():
    log.debug('...loading mapping for %s', mapping)
    the_dict = mappings[mapping] = {}
    for user_id in user_ids:
      new_id = getter(user_id)
      if new_id:
        the_dict[new_id] = user_id
      

def _sanity_check_args(streams, user_id_mappers):
  assert len(streams) > 1 # Must have more than one stream for funnel analysis.
  assert streams[0].invert == False # Can't handle invert=True in first stream.
  for stream in streams:
    if stream.invert: # If invert is true, can not specify output fields.
      assert not stream.output_fields
  first_stream_user_id = streams[0].user_field
  required_mappings = ({stream.user_field for stream in streams} -
                       {first_stream_user_id})
  # Filter down mappings to only the ones we'll need to execute here.
  user_id_mappers = {required: user_id_mappers[required]
                     for required in required_mappings}
  return streams, user_id_mappers


def funnel_analyze(client, streams, start, end, end_first_funnel_step,
                   user_id_mappers, user_filter,
                   fuzzy_time=timedelta(minutes=5)):
  """
  `streams`: a list of FunnelStep objects, each representing a step in the
  funnel.  The funnel is composed from these objects.

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
  funnel_output = FunnelOutput()
  user_id_mappings = {}

  for idx, stream in enumerate(streams):
    log.debug('Processing stream', stream.stream_name)
    step_end = end
    if idx == 0:
      user_id_mappings[stream.user_field] = IdentityDict()
      step_end = end_first_funnel_step
    output = _stream_earliest_action(
      client, stream, start, step_end,
      fuzzy_time, last_user_action, user_id_mappings)
    funnel_output.add(output)
    last_user_action = output['user_action']
    # For the first stream in the funnel, load the mappings to other
    # user_id formats we'll find in subsequent streams.
    if idx == 0:
      log.debug('Loading user_id mappings')
      _load_user_id_mappings(user_id_mappings, user_id_mappers, last_user_action)

  return funnel_output
