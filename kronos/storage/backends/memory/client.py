import bisect

from collections import defaultdict
from uuid import UUID

from kronos.core.validators import ID_FIELD, TIMESTAMP_FIELD
from kronos.storage.backends import BaseStorage

class Event(dict):
  def __cmp__(self, other):
    self_uuid = UUID(self[ID_FIELD])
    other_uuid = UUID(other[ID_FIELD])
    if self_uuid.time < other_uuid.time:
      return -1
    elif self_uuid.time > other_uuid.time:
      return 1
    else:
      if self_uuid.bytes < other_uuid.bytes:
        return -1
      elif self_uuid.bytes > other_uuid.bytes:
        return 1
    return 0

class InMemoryStorage(BaseStorage):
  CONF_PARAMETERS = {
    'max_items': int
  }
  
  def __init__(self, name, **settings):
    super(InMemoryStorage, self).__init__(name, **settings)
    self.default_max_items = int(settings['default_max_items'])
    self.db = defaultdict(list)

  def is_alive(self):
    return True
    
  def insert(self, stream, events, configuration):
    max_items = configuration.get('max_items', self.default_max_items)    
    for event in events:
      while len(self.db[stream]) >= max_items:
        self.db[stream].pop(0)
      bisect.insort(self.db[stream], Event(event))
      
  def _retrieve(self, stream, start_id, end_time, configuration):
    events_returned = 0
    start_id_event = Event({ID_FIELD: str(start_id)})
    for event in self.db[stream]:
      if event <= start_id_event:
        continue
      if event[TIMESTAMP_FIELD] >= end_time:
        break
      yield event
      events_returned += 1
