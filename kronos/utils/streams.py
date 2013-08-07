import time

from kronos.constants.order import ResultOrder
from kronos.storage import router
from kronos.utils.math import time_to_kronos_time

def get_stream_properties(stream):
  #TODO(meelap): Do this more smartly.
  now = time_to_kronos_time(time.time())
  backend, configuration = router.backend_to_retrieve(stream)
  events = backend.retrieve(stream, 0, now, None, ResultOrder.DESCENDING,
                            configuration)
  try:
    properties = events.next().iterkeys()
    return filter(lambda p: not p.startswith('@'), properties)
  except StopIteration:
    return []

