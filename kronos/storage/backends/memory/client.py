import bisect

from collections import defaultdict
from uuid import UUID

from kronos.conf import settings
ID_FIELD = settings.stream['fields']['id']
TIMESTAMP_FIELD = settings.stream['fields']['timestamp']

from kronos.storage.backends import BaseStorage


class Event(dict):
  """
  An event is stored in memory as a dictionary.
  We define a comparator because events are sortable by the time in their
  UUIDs
  """
  def __cmp__(self, other):
    self_uuid = UUID(self[ID_FIELD])
    other_uuid = UUID(other[ID_FIELD])

    # If my time is != other's time, return that comparison
    if self_uuid.time < other_uuid.time:
      return -1
    elif self_uuid.time > other_uuid.time:
      return 1

    # If our times are equal, compare our raw bytes
    if self_uuid.bytes < other_uuid.bytes:
      return -1
    elif self_uuid.bytes > other_uuid.bytes:
      return 1

    return 0

class InMemoryStorage(BaseStorage):
  """
  The in memory storage backend maintains a sorted list of events per stream
  name.
  The size of this list is capped at max_items|default_max_items per stream.
  """

  CONF_PARAMETERS = { 'max_items': int }
  
  def __init__(self, name, **settings):
    super(InMemoryStorage, self).__init__(name, **settings)
    self.default_max_items = int(settings['default_max_items'])
    self.db = defaultdict(list)

  def is_alive(self):
    return True
  
  def insert(self, stream, events, configuration):
    """
    `stream` is the name of a stream and `events` is a list of events to
    insert. Make room for the events to insert if necessary by deleting the
    oldest events. Then insert each event in time sorted order.
    """

    max_items = configuration.get('max_items', self.default_max_items)  
    for event in events:
      while len(self.db[stream]) >= max_items:
        self.db[stream].pop(0)
      bisect.insort(self.db[stream], Event(event))
    
  def _retrieve(self, stream, start_id, end_time, max_items, configuration):
    """
    Return at most `max_items` events associated with `stream`, starting
    with `start_id` and occuring before `end_time`.
    """

    events_returned = 0
    start_id_event = Event({ID_FIELD: str(start_id)})
    for event in self.db[stream]:
      if event <= start_id_event:
        continue
      if event[TIMESTAMP_FIELD] >= end_time:
        break
      yield event
      events_returned += 1
      if events_returned == max_items:
        break
