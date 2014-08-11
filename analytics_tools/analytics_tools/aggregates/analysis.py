from pykronos.common.event_tools import get_property
from pykronos.client import TIMESTAMP_FIELD
from pykronos.common.time import kronos_time_to_epoch_time


def aggregate_stream(events, aggregator_class, field, bucket_width):
  current_bucket, aggregator = None, None
  emitted = False
  for event in events:
    timestamp = kronos_time_to_epoch_time(event[TIMESTAMP_FIELD])
    bucket = timestamp - (timestamp % bucket_width)
    if bucket != current_bucket:
      if current_bucket is not None:
        yield current_bucket, aggregator.aggregate()
        emitted = True
      current_bucket = bucket
      aggregator = aggregator_class()
      emitted = False
    aggregator.update(get_property(event, field))
  if not emitted and current_bucket and aggregator:
    yield current_bucket, aggregator.aggregate()
