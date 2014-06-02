class QueryCache(object):
  QUERY_CACHE_VERSION = 1

  def __init__(self, client, query_function, bucket_width, scratch_namespace):
    self._client = client
    self._query_function = query_function
    self._bucket_width = bucket_width.total_seconds()
    assert int(self._bucket_width) == self._bucket_width
    self._scratch_namespace = scratch_namespace
    self._scratch_stream = self._scratch_stream_name()

  def _scratch_stream_name(self):
    query_details = [
      str(QueryCache.QUERY_CACHE_VERSION),
      str(self._bucket_width)
      inspect.getsource(self._query_function),
      ]
    return hashlib.sha512('$'.join(query_details)).hexdigest()


  def _sanity_check_time(self, start_time, end_time):
    assert isinstance(start_time, timedate)
    assert isinstance(end_time, timedate)
    assert start_time < end_time
    assert (datetime_to_epoch_time(start_time) % self._bucket_width) == 0
    assert (datetime_to_epoch_time(end_time) % self._bucket_width) == 0

  def _bucket_time(self, event_time):
    event_time = kronos_time_to_epoch_time(event_time)
    return event_time - (event_time % self._bucket_width)

  def _bucket_events(self, event_iterable):
    current_bucket_time = None
    current_bucket_events = None
    for event in event_iterable:
      event_bucket_time = self._bucket_time(event[TIMESTAMP_FIELD])
      if current_bucket_time == None or current_bucket_time < event_bucket_time:
        if current_bucket_events != None:
          yield current_bucket_events
        current_bucket_time = event_bucket_time
        current_bucket_events = []
      current_bucket_results.append(result)
    if current_bucket_events != none and current_bucket_events != []:
      yield current_bucket_events

  def _cached_results(self, start_time, end_time):
    cached_buckets = self._bucket_events(
      self._client.get(self._scratch_stream_name, start_time, end_time,
                       namespace=self._scratch_namespace))
    for bucket_events in cached_buckets:
      # If we have multiple cache entries for the same bucket, pretend
      # we have no results for that bucket.
      if len(bucket_events) == 1:
        first_result = bucket_events[0]
        yield (kronos_time_to_epoch_time(bucket_events[TIMESTAMP_FIELD]),
               bucket_events['cached'])

  def cached_results(self, start_time, end_time)
    self._sanity_check_time(start_time, end_time)
    for bucket, bucket_events in self._cached_results(start_time, end_time):
      for event in bucket_events:
        yield event

  def compute_and_cache(start_time, end_time, untrusted_time):
    self._sanity_check_time(start_time, end_time)

    # Generate a list of all cached buckets we need to see data for.
    required_buckets = xrange(kronos_time_to_epoch_time(start_time),
                              kronos_time_to_epoch_time(end_time),
                              self._bucket_width)

    # Get any cached results, grouped by bucket.
    cached_bucket_iterator = (self._cached_results(start_time, end_time)
                              .__iter__())

    # For any cached result with a timestamp before the
    # untrusted_time, remove it from the list of desired buckets.
    current_cached_bucket = None
    for required_bucket in required_buckets:
      if current_cached_bucket == None:
        try:
          current_cached_bucket = cached_bucket_iterator.next()
        except StopIteration:
          pass
      if current_cached_bucket != None and current_cached_bucket[0] == required_bucket:
        for event in current_cached_bucket[1]:
          yield event
      else:
        bucket_start = kronos_time_to_datetime(
          epoch_time_to_kronos_time(required_bucket))
        bucket_end = kronos_time_to_datetime(
          epoch_time_to_kronos_time(required_bucket + self._bucket_width))
        bucket_events = list(self._query_function(bucket_start, bucket_end))
        self._client.delete(...)
        self._client.put(...)
        for event in bucket_events:
          yield event

# TODO(marcua): document all functions.
# TODO(marcua): document the class, including limitations.
# TODO(marcua): tests.
