import bisect

from collections import defaultdict
from uuid import UUID

from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import ResultOrder
from kronos.storage.backends import BaseStorage
from kronos.utils.math import uuid_from_kronos_time
from kronos.utils.math import UUIDType


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
  The size of this list is capped at `max_items` per stream.
  """

  SETTINGS_VALIDATORS = {
    'default_max_items': lambda x: int(x) > 0,
  }
  
  def __init__(self, name, **settings):
    super(InMemoryStorage, self).__init__(name, **settings)
    self.db = defaultdict(lambda: defaultdict(list))

  def is_alive(self):
    return True
  
  def _insert(self, namespace, stream, events, configuration):
    """
    `stream` is the name of a stream and `events` is a list of events to
    insert. Make room for the events to insert if necessary by deleting the
    oldest events. Then insert each event in time sorted order.
    """
    max_items = configuration.get('max_items', self.default_max_items)  
    for event in events:
      while len(self.db[namespace][stream]) >= max_items:
        self.db[namespace][stream].pop(0)
      bisect.insort(self.db[namespace][stream], Event(event))
    
  def _delete(self, namespace, stream, start_id, end_time, configuration):
    """
    Delete events with id > `start_id` and end_time <= `end_time`.
    """
    start_id = str(start_id)
    start_id_event = Event({ID_FIELD: start_id})
    end_id_event = Event({ID_FIELD:
                          str(uuid_from_kronos_time(end_time,
                                                    _type=UUIDType.HIGHEST))})
    stream_events = self.db[namespace][stream]

    # Find the interval our events belong to.
    lo = bisect.bisect_left(stream_events, start_id_event)
    if lo + 1 > len(stream_events):
      return 0
    if stream_events[lo][ID_FIELD] == start_id:
      lo += 1
    hi = bisect.bisect_right(stream_events, end_id_event)

    del stream_events[lo:hi]
    return max(0, hi - lo)

  def _retrieve(self, namespace, stream, start_id, end_time, order, limit,
                configuration):
    """
    Yield events from stream starting after the event with id `start_id` until
    and including events with timestamp `end_time`.
    """
    start_id = str(start_id)
    start_id_event = Event({ID_FIELD: start_id})
    end_id_event = Event({ID_FIELD:
                          str(uuid_from_kronos_time(end_time,
                                                    _type=UUIDType.HIGHEST))})
    stream_events = self.db[namespace][stream]

    # Find the interval our events belong to.
    lo = bisect.bisect_left(stream_events, start_id_event)
    if lo + 1 > len(stream_events):
      return
    if stream_events[lo][ID_FIELD] == start_id:
      lo += 1
    hi = bisect.bisect_right(stream_events, end_id_event)
    
    if order == ResultOrder.DESCENDING:
      index_it = xrange(hi-1, lo-1, -1)
    else:
      index_it = xrange(lo, hi)

    for i in index_it:
      if limit <= 0:
        break
      limit -= 1
      yield stream_events[i]

  def _streams(self, namespace):
    return self.db[namespace].iterkeys()
