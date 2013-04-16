import uuid

from kronos.core.exceptions import ImproperlyConfigured

class BaseStorage(object):
  def __init__(self, name, **settings):
    self.name = name

  def is_alive(self):
    raise NotImplementedError("Must implement `is_alive` method for %s" %
                              self.__class__.__name__)    

  def insert(self, stream, events, configuration):
    raise NotImplementedError("Must implement `insert` method for %s" %
                              self.__class__.__name__)

  def retrieve(self, stream, start_time, end_time, start_id, configuration):
    """
    Retrieves all the events for `stream` from `start_time` (inclusive) till
    `end_time` (exclusive). Alternatively to `start_time`, `start_id` can be 
    provided, and then all events from `start_id` (exclusive) till `end_time`
    (exlusive) are returned. `start_id` should be used in cases when the client
    got disconnected from the server before all the events in the requested
    time window had been returned. 
    """
    if not start_id:
      # Make the lowest value UUID with the start timestamp.
      clock_seq_low = 0 & 0xffL
      clock_seq_hi_variant = 0 & 0x3fL
      node = 0 & 0xffffffffffffL
      nanoseconds = int(start_time * 1e9)
      timestamp = int(nanoseconds/100) + 0x01b21dd213814000L
      time_low = timestamp & 0xffffffffL
      time_mid = (timestamp >> 32L) & 0xffffL
      time_hi_version = (timestamp >> 48L) & 0x0fffL
      start_id = uuid.UUID(fields=(time_low, time_mid, time_hi_version,
                                   clock_seq_hi_variant, clock_seq_low, node),
                           version=1)
    else:
      start_id = uuid.UUID(start_id)
    return self._retrieve(stream, start_id, end_time, configuration)
  
  def _retrieve(self, stream, start_id, end_time, configuration):
    raise NotImplementedError("Must implement `retrieve` method for %s." %
                              self.__class__.__name__)

  def validate_configuration(self, stream_configuration):
    if stream_configuration:
      if not isinstance(stream_configuration, dict):
        raise ImproperlyConfigured('%s: stream configuration must be a dict.' %
                                   self.__class__)
      invalid_keys = (set(stream_configuration.keys()) -
                      set(self.CONF_PARAMETERS.keys()))
      if invalid_keys:
        raise ImproperlyConfigured('%s: stream configuration contains invalid '
                                   'key %s.' % (self.__class__, invalid_keys[0]))
      for key in stream_configuration:
        try:
          self.CONF_PARAMETERS[key](stream_configuration[key])
        except:
          raise ImproperlyConfigured('%s: stream configuration contains '
                                     'invalid value for key %s.' %
                                     (self.__class__, key))
