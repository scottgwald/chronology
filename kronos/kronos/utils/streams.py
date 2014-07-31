import time

from kronos.common.time import epoch_time_to_kronos_time
from kronos.conf.constants import ResultOrder
from kronos.storage.router import router

def get_stream_properties(namespace, stream):
  # TODO(meelap): Do this more smartly.
  now = epoch_time_to_kronos_time(time.time())
  backend, configuration = router.backend_to_retrieve(namespace, stream)
  events = backend.retrieve(namespace, stream, 0, now, None, configuration,
                            order=ResultOrder.DESCENDING, limit=1)
  try:
    return filter(lambda p: not p.startswith('@'), events.next().iterkeys())
  except StopIteration:
    return []

