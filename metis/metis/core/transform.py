import inspect
import json
import re
import sys
import types

from metis.conf import constants

TRANSFORM_MAP = {}


def _expand_args(func):
  def wrapper(args):
    assert isinstance(args, tuple)
    assert len(args) == 2
    assert isinstance(args[0], types.StringType)
    return func(*args)
  return wrapper


def _get_key_value(arg):
  if isinstance(arg, tuple):
    assert len(arg) == 2
    arg, value = arg
    assert isinstance(arg, types.StringType)
    return (arg, value)
  return (None, arg)


def parse(value):
  if not 'transform' in value:
    raise ValueError
  if not TRANSFORM_MAP:
    # Create transform name to class map.
    for name, obj in inspect.getmembers(sys.modules[__name__]):
      if (obj == Transform or
          not inspect.isclass(obj) or
          not issubclass(obj, Transform)):
        continue
      TRANSFORM_MAP[obj.get_name()] = obj
  if value['transform'] not in TRANSFORM_MAP:
    raise ValueError
  return TRANSFORM_MAP[value['transform']](**value)


class Transform(object):
  NAME = None # Optional override of name.

  @classmethod
  def get_name(cls):
    if cls.NAME is not None:
      return str(cls.NAME).upper()
    return cls.__name__.replace('Transform', '').upper()

  def to_dict(self):
    return {'transform': self.get_name()}

  def apply(self, spark_context, rdd):
    raise NotImplemented


class NullTransform(Transform):
  def apply(self, rdd):
    return rdd


class ProjectionTransform(Transform):
  def __init__(self, keys, **kwargs):
    self.keys = set(keys)
    # Always project id and timestamp fields.
    self.keys.add(constants.ID_FIELD)
    self.keys.add(constants.TIMESTAMP_FIELD)
    
  def to_dict(self):
    dictionary = super(ProjectionTransform, self).serialize()
    dictionary['keys'] = list(self.keys)
    return dictionary

  def _project(self, arg):
    key, event = _get_key_value(arg)
    event = dict((key, value) for key, value in event.iteritems()
                 if key in self.keys)
    if key is None:
      return event
    return (key, event)

  def apply(self, spark_context, rdd):
    return rdd.map(self._project)


class FilterTransform(Transform):
  def __init__(self, key, op, value, **kwargs):
    # TODO(usmanm): Validate op & value.
    self.key = key
    self.op = op
    self.value = value

  def to_dict(self):
    dictionary = super(FilterTransform, self).serialize()
    dictionary.update({'key': self.key,
                       'op': self.op, 
                       'value': self.value})
    return dictionary

  def _safe_lambda(self, filter_function, _type=None):
    """ Factory for functions which do sanity checks on each event before
    passing it to the filter function in it. """
    def func(arg):
      _, event = _get_key_value(arg)
      if self.key not in event:
        return False
      if _type and not isinstance(event[self.key], _type):
        return False
      try:
        return filter_function(event)
      except:
        # return False in case anything fails.
        return False
    return func
  
  def apply(self, spark_context, rdd):
    if self.op == 'lt':
      return rdd.filter(
          self._safe_lambda(lambda event: event[self.key] < self.value))
    if self.op == 'lte':
      return rdd.filter(
          self._safe_lambda(lambda event: event[self.key] <= self.value))
    if self.op == 'gt':
      return rdd.filter(
          self._safe_lambda(lambda event: event[self.key] > self.value))
    if self.op == 'gte':
      return rdd.filter(
          self._safe_lambda(lambda event: event[self.key] >= self.value))
    if self.op == 'eq':
      return rdd.filter(
          self._safe_lambda(lambda event: event[self.key] == self.value))
    if self.op == 'neq':
      return rdd.filter(
          self._safe_lambda(lambda event: event[self.key] != self.value))
    if self.op == 'contains':
      return rdd.filter(
          self._safe_lambda(lambda event: self.value in event[self.key],
                            _type=list))
    if self.op == 'in':
      return rdd.filter(
          self._safe_lambda(lambda event: event[self.key] in self.value))
    if self.op == 'regex':
      return rdd.filter(
          self._safe_lambda(lambda event: re.search(self.value, event[self.key]),
                            _type=types.StringType))


class GroupByTimeTransform(Transform):
  def __init__(self, time_width, **kwargs):
    self.time_width = time_width

  def to_dict(self):
    dictionary = super(GroupByTimeTransform, self).serialize()
    dictionary['time_width'] = self.time_width
    return dictionary

  def _map_into_time_buckets(self, arg):
    bucket, event = _get_key_value(arg)
    time = event[constants.TIMESTAMP_FIELD]
    # Ceiling of `time` when rounded by `self.time_width`.
    time_bucket = [{constants.TIMESTAMP_FIELD: 
                    int(time - (time % self.time_width) + self.time_width)}]
    if bucket is not None:
      bucket = json.loads(bucket)
      # Time bucket should always be first *key*.
      time_bucket.extend(bucket)
    return (json.dumps(time_bucket), event)

  def apply(self, spark_context, rdd):
    return rdd.map(self._map_into_time_buckets)


class OrderByTransform(Transform):
  def __init__(self, keys, reverse=False, **kwargs):
    assert len(set(keys)) == len(keys)
    self.keys = keys
    self.reverse = reverse

  def to_dict(self):
    dictionary = super(OrderByTransform, self).serialize()
    dictionary['keys'] = self.keys
    return dictionary

  def apply(self, spark_context, rdd):
    # TODO(usmanm): Is there a more efficient way to do this? PySpark doesn't
    # seem to have a notion of OrderedRDD (which exists in Scala land and 
    # supports a `sortByKey` operation).
    first = rdd.first()
    key, event = _get_key_value(first)
    cmp_function = lambda x, y: cmp(tuple(x.get(key) for key in self.keys),
                                    tuple(y.get(key) for key in self.keys))
    if key is None:
      events = sorted(rdd.collect(), cmp_function, reverse=self.reverse)
      return spark_context.parallelize(events)
    grouped_map = rdd.groupByKey().collectAsMap()
    def iterator():
      for key, values in grouped_map.iteritems():
        values = sorted(values, cmp_function, reverse=self.reverse)
        yield (key, values)
    return spark_context.parallelize(iterator())


class LimitTransform(Transform):
  def __init__(self, limit, **kwargs):
    self.limit = int(limit)

  def to_dict(self):
    dictionary = super(LimitTransform, self).serialize()
    dictionary['limit'] = self.limit
    return dictionary

  def apply(self, spark_context, rdd):
    # TODO(usmanm): Is there a more efficient way to do this?
    first = rdd.first()
    key, event = _get_key_value(first)
    if key is None:
      return spark_context.parallelize(rdd.take(self.limit))
    if not isinstance(event, list):
      rdd = rdd.groupAsKey()
    return rdd.flatMap(
        _expand_args(lambda key, values:
                       [(key, value) for value in values[:self.limit]]))


class GroupByTransform(Transform):
  def __init__(self, keys, **kwargs):
    assert len(set(keys)) == len(keys)
    self.keys = keys
    
  def to_dict(self):
    dictionary = super(GroupByTransform, self).serialize()
    dictionary['keys'] = self.keys
    return dictionary

  def apply(self, spark_context, rdd):
    def _map(arg):
      bucket, event = _get_key_value(arg)
      if bucket is None:
        return (json.dumps([{key: event.get(key)} for key in self.keys]), event)
      bucket = json.loads(bucket)
      bucket.extend({key: event.get(key)} for key in self.keys)
      return (json.dumps(bucket), event)
    return rdd.map(_map)


class AggregateTransform(Transform):
  def __init__(self, aggregates, **kwargs):
    for aggregate in aggregates:
      if aggregate.get('key') is None:
        # Only allow `count` aggregate if aggregating over entire event tuple.
        assert aggregate['op'] == 'count'
    self.aggregates = aggregates
    self.canonical_key_to_alias = {}
    for aggregate in aggregates:
      if aggregate.get('alias') is None:
        continue
      self.canonical_key_to_alias[
          (aggregate['op'], aggregate.get('key'))] = str(aggregate['alias'])

  def to_dict(self):
    dictionary = super(AggregateTransform, self).serialize()
    dictionary['aggregates'] = self.aggregates
    return dictionary

  def apply(self, spark_context, rdd):
    @_expand_args
    def _map(key, event):
      value = {}
      for aggregate in self.aggregates:
        c_key = (aggregate['op'], aggregate.get('key'))
        op, event_key = c_key
        if op == 'count':
          value[c_key] = 1 if (event_key in event or event_key is None) else 0
        elif op == 'sum':
          value[c_key] = event.get(event_key, 0)
        elif op == 'min':
          value[c_key] = event.get(event_key, float('inf'))
        elif op == 'max':
          value[c_key] = event.get(event_key, -float('inf'))
        elif self.op == 'avg':
          value[c_key] = (event[event_key], 1) if event_key in event else (0, 0)
      return (key, value)

    def _reduce(value1, value2):
      assert isinstance(value1, dict)
      assert isinstance(value2, dict)
      assert set(value1) == set(value2)
      value = {}
      for c_key in value1:
        op = c_key[0]
        if op in ('count', 'sum'):
          value[c_key] = value1[c_key] + value2[c_key]
        elif op == 'min':
          value[c_key] = min(value1[c_key], value2[c_key])
        elif op == 'max':
          value[c_key] = max(value1[c_key], value2[c_key])
        elif self.op == 'avg':
          value[c_key] = (value1[c_key][0] + value2[c_key][0],
                          value1[c_key][1] + value2[c_key][1])
      return value

    @_expand_args
    def _map2(buckets, value):
      c_keys = value.keys()
      for c_key in c_keys:
        result = value[c_key]
        op = c_key[0]
        if op == 'average':
          value[c_key] = (result[0]/float(result[1]) if result[1]
                                  else None)
        if c_key in self.canonical_key_to_alias:
          new_key = self.canonical_key_to_alias[c_key]
        elif c_key[1] is None:
          new_key = op
        else:
          new_key = '%s(%s)' % c_key
        value[new_key] = value[c_key]
        del value[c_key]            
      buckets = json.loads(buckets)
      assert len(buckets) > 0
      for bucket in buckets:
        value.update(bucket)
      return value

    return (rdd
            .map(_map)
            .reduceByKey(_reduce)
            .map(_map2))
