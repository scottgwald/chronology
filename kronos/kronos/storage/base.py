import sys
import uuid

from kronos.conf.constants import ResultOrder
from kronos.utils.math import uuid_from_kronos_time
from kronos.utils.math import uuid_to_kronos_time
from kronos.utils.math import UUIDType


class BaseStorage(object):
  # All subclasses must define `SETTINGS_VALIDATORS` mapping option names to a
  # function that takes a value for that option and returns True if it is valid
  # and False otherwise. For example, if a subclass takes one option called
  # `max_size` that should be a nonnegative integer, we would have:
  # SETTINGS_VALIDATORS = { 'max_size': lambda x: int(x) >= 0 }
  SETTINGS_VALIDATORS = {}

  def __init__(self, name, **settings):
    """
    Subclasses can assume that `settings` only contains keys that are also in
    `SETTINGS_VALIDATORS` and that their values are valid.
    """
    self.name = name
    for setting in self.__class__.SETTINGS_VALIDATORS:
      setattr(self, setting, settings[setting])
      assert self.__class__.SETTINGS_VALIDATORS[setting](getattr(self, setting))

  def is_alive(self):
    raise NotImplementedError('Must implement `is_alive` method for %s' %
                              self.__class__.__name__)    

  def insert(self, namespace, stream, events, configuration):
    self._insert(namespace, stream, events, configuration)

  def _insert(self, namespace, stream, events, configuration):
    raise NotImplementedError('Must implement `_insert` method for %s' %
                              self.__class__.__name__)

  def delete(self, namespace, stream, start_time, end_time, start_id,
             configuration):
    if not start_id:
      start_id = uuid_from_kronos_time(start_time, _type=UUIDType.LOWEST)
    else:
      start_id = uuid.UUID(start_id)
    if uuid_to_kronos_time(start_id) > end_time:
      return 0      
    return self._delete(namespace, stream, start_id, end_time, configuration)

  def _delete(self, stream, start_id, end_time, configuration, namespace):
    raise NotImplementedError('Must implement `_delete` method for %s' %
                              self.__class__.__name__)

  def retrieve(self, namespace, stream, start_time, end_time, start_id,
               configuration, order=ResultOrder.ASCENDING, limit=sys.maxint):
    """
    Retrieves all the events for `stream` from `start_time` (inclusive) till
    `end_time` (exclusive). Alternatively to `start_time`, `start_id` can be 
    provided, and then all events from `start_id` (exclusive) till `end_time`
    (exlusive) are returned. `start_id` should be used in cases when the client
    got disconnected from the server before all the events in the requested
    time window had been returned. `order` can be one of ResultOrder.ASCENDING
    or ResultOrder.DESCENDING.
    """
    if not start_id:
      start_id = uuid_from_kronos_time(start_time, _type=UUIDType.LOWEST)
    else:
      start_id = uuid.UUID(start_id)
    if uuid_to_kronos_time(start_id) > end_time:
      return []
    return self._retrieve(namespace, stream, start_id, end_time, order, limit, 
                          configuration)
  
  def _retrieve(self, namespace, stream, start_id, end_time, order, limit,
                configuration):
    raise NotImplementedError('Must implement `_retrieve` method for %s.' %
                              self.__class__.__name__)

  def streams(self, namespace):
    return self._streams(namespace)

  def _streams(self, namespace):
    raise NotImplementedError('Must implement `_streams` method for %s' %
                              self.__class__.__name__)
