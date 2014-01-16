import inspect
import json
import sys
import types

from metis.conf import constants
from metis.core.query.enums import (AggregateType,
                                    OperatorType,
                                    ValueType)
from metis.core.query.utils import (cast_to_number,
                                    Counter,
                                    get_value,
                                    generate_filter,
                                    validate_condition,
                                    validate_getter)


class Operator(object):
  OPERATORS = None # operator name => operator class
  OPERATOR_TYPE = None

  @classmethod
  def get_name(cls):
    assert cls.OPERATOR_TYPE is not None
    return cls.OPERATOR_TYPE

  @classmethod
  def parse(cls, op_dict):
    if Operator.OPERATORS is None:
      Operator.OPERATORS = {}
      # Create operator name to class map.
      for name, obj in inspect.getmembers(sys.modules[__name__]):
        if (obj == Operator or
            not inspect.isclass(obj) or
            not issubclass(obj, Operator)):
          continue
        Operator.OPERATORS[obj.get_name()] = obj
      # Ensure that we have an Operator class for each `OperatorType`.
      assert set(Operator.OPERATORS) == OperatorType.values()
    assert isinstance(op_dict, dict)
    # The stream operator dict maybe wrapped into a {stream: ..., alias: ...}.
    if 'operator' not in op_dict:
      assert 'stream' in op_dict
      op_dict = op_dict['stream']
    assert op_dict.get('operator') in Operator.OPERATORS
    return Operator.OPERATORS[op_dict['operator']](**op_dict)

  def get_rdd(self, spark_context):
    ''' Always returns an RDD for a valid event collection. '''
    raise NotImplemented


class KronosOperator(Operator):
  OPERATOR_TYPE = OperatorType.KRONOS
  
  def __init__(self, stream, start_time, end_time, host, namespace=None,
               **kwargs):
    assert isinstance(start_time, int)
    assert isinstance(end_time, int)
    assert namespace is None or isinstance(namespace, types.StringTypes)
    # TODO(usmanm): Validate `host`.
    assert isinstance(stream, types.StringTypes)

    self.start_time = start_time
    self.end_time = end_time
    self.host = host
    self.namespace = namespace
    self.stream = stream

  def get_rdd(self, spark_context):
    # TODO(usmanm): Fix this to do parallel reads from Kronos.
    from pykronos import KronosClient

    client = KronosClient(self.host, blocking=True)
    events = client.get(self.stream,
                        self.start_time,
                        self.end_time,
                        namespace=self.namespace)
    return spark_context.parallelize(events)


class ProjectOperator(Operator):
  '''
  Creates a projection of the event using `fields`. If `merge` is True then
  the original event is not dropped and `fields` are only updated/added to the
  event.
  '''
  OPERATOR_TYPE = OperatorType.PROJECT

  def __init__(self, stream, fields, merge=False, **kwargs):
    for field, getter in fields.iteritems():
      assert isinstance(field, types.StringTypes)
      validate_getter(getter)

    self.merge = bool(merge)
    self.fields = fields
    self.stream = Operator.parse(stream)

  def _project(self, event):
    if self.merge:
      projection = event.copy()
    else:
      # Always project `TIMESTAMP_FIELD`.
      projection = {constants.TIMESTAMP_FIELD:
                    event[constants.TIMESTAMP_FIELD]}

    for field, getter in self.fields.iteritems():
      projection[field] = get_value(event, getter)

    return projection

  def get_rdd(self, spark_context):
    return self.stream.get_rdd(spark_context).map(self._project)


class FilterOperator(Operator):
  OPERATOR_TYPE = OperatorType.FILTER

  def __init__(self, stream, condition, **kwargs):
    validate_condition(condition)

    self.condition = condition
    self.stream = Operator.parse(stream)

  def get_rdd(self, spark_context):
    return (self.stream.get_rdd(spark_context)
            .filter(generate_filter(self.condition)))


class OrderByOperator(Operator):
  OPERATOR_TYPE = OperatorType.ORDER_BY

  def __init__(self, stream, fields, reverse=False, **kwargs):
    for field in fields:
      validate_getter(field)

    self.fields = fields
    self.reverse = bool(reverse)
    self.stream = Operator.parse(stream)

  def _cmp_function(self, eventA, eventB):
    return cmp(tuple(get_value(eventA, field) for field in self.fields),
               tuple(get_value(eventB, field) for field in self.fields))

  def get_rdd(self, spark_context):
    # TODO(usmanm): Is there a more efficient way to do this? PySpark doesn't
    # seem to have a notion of OrderedRDD (which exists in Scala land and
    # supports a `sortByKey` operation).
    events = sorted(self.stream.get_rdd(spark_context).collect(),
                    self._cmp_function, reverse=self.reverse)
    return spark_context.parallelize(events)


class LimitOperator(Operator):
  OPERATOR_TYPE = OperatorType.LIMIT

  def __init__(self, stream, limit, **kwargs):
    assert isinstance(limit, int)
    assert limit > 0
    
    self.limit = limit
    self.stream = Operator.parse(stream)

  def get_rdd(self, spark_context):
    # TODO(usmanm): Is there a more efficient way to do this?
    return spark_context.parallelize(self.stream.get_rdd(spark_context)
                                     .take(self.limit))


class AggregateOperator(Operator):
  '''
  Groups the stream based the `group` (supports multi-key grouping) and then
  runs the `aggregates` to output a new Kronos stream which has only
  the aggregated values.
  '''
  OPERATOR_TYPE = OperatorType.AGGREGATE

  def __init__(self, stream, groups, aggregates, **kwargs):
    time_seen_in_groups = False
    for group, getter in groups.iteritems():
      validate_getter(getter)
      if (getter['type'] == ValueType.PROPERTY and
          getter['name'] == constants.TIMESTAMP_FIELD):
        time_seen_in_groups = True
    counter = Counter()
    time_seen_in_aggregates = False
    aliases = set()
    for aggregate in aggregates:
      args = aggregate.get('args', [])
      assert isinstance(args, list)
      for arg in args:
        validate_getter(arg)
      assert aggregate.get('op') in AggregateType.values()
      if not aggregate.get('alias'):
        aggregate['alias'] = '%s_%s' % (aggregate['op'], counter.increment())
      if aggregate['alias'] == constants.TIMESTAMP_FIELD:
        time_seen_in_aggregates = True
      if aggregate['alias'] in aliases:
        raise ValueError
      aliases.add(aggregate['alias'])
    # If there's no grouping over the time field or no aggregate with that
    # alias, raise an error.
    if not (time_seen_in_groups or time_seen_in_aggregates):
      raise ValueError
    self.groups = groups
    self.aggregates = aggregates
    self.stream = Operator.parse(stream)

  def _group(self, event):
    # `key` can only be strings in Spark if you want to use `reduceByKey`.
    new_event = {group: get_value(event, getter)
                 for group, getter in self.groups.iteritems()}
    key = json.dumps(new_event)
    for aggregate in self.aggregates:
      args = aggregate.get('args', [])
      if aggregate['op'] == AggregateType.COUNT:
        assert len(args) in (0, 1)
        if not len(args):
          value = 1
        else:
          value = 0 if get_value(event, args[0]) is None else 1
      elif aggregate['op'] == AggregateType.SUM:
        assert len(args) == 1
        value = cast_to_number(get_value(event, args[0]), 0)
      elif aggregate['op'] == AggregateType.MIN:
        assert len(args) == 1
        value = cast_to_number(get_value(event, args[0]), float('inf'))
      elif aggregate['op'] == AggregateType.MAX:
        assert len(args) == 1
        value = cast_to_number(get_value(event, args[0]), -float('inf'))
      elif aggregate['op'] == AggregateType.AVG:
        assert len(args) == 1
        value = cast_to_number(get_value(event, args[0]), None)
        if value is None:
          value = (0, 0)
        else:
          value = (value, 1)
      elif aggregate['op'] == AggregateType.VALUE_COUNT:
        assert len(args) > 0
        if len(args) == 1:
          value_key = str(get_value(event, args[0]))
        else:
          value_key = str(tuple(get_value(event, arg) for arg in args))
        value = {value_key: 1}
      else:
        raise ValueError
      new_event[aggregate['alias']] = value
    return (key, new_event)

  def _reduce(self, event1, event2):
    event = event1.copy()
    for aggregate in self.aggregates:
      alias = aggregate['alias']
      if aggregate['op'] in (AggregateType.COUNT, AggregateType.SUM):
        value = event1[alias] + event2[alias]
      elif aggregate['op'] == AggregateType.MIN:
        value = min(event1[alias], event2[alias])
      elif aggregate['op'] == AggregateType.MAX:
        value = max(event1[alias], event2[alias])
      elif aggregate['op'] == AggregateType.AVG:
        value = (event1[alias][0] + event2[alias][0],
                 event1[alias][1] + event2[alias][1])
      elif aggregate['op'] == AggregateType.VALUE_COUNT:
        value = event1[alias].copy()
        for key in event2[alias]:
          value.setdefault(key, 0)
          value[key] += event2[alias][key]
      else:
        raise ValueError
      event[alias] = value
    return event

  def _finalize(self, event):
    # `event` is of the form (key, event).
    event = event[1].copy()
    for aggregate in self.aggregates:
      if aggregate['op'] == AggregateType.AVG:
        alias = aggregate['alias']
        value = event[alias]
        if not value[1]:
          event[alias] = None
        else:
          event[alias] = value[0] / float(value[1])
    return event

  def get_rdd(self, spark_context):
    # This will not preserve sorting on `TIMESTAMP_FIELD`.
    return (self.stream.get_rdd(spark_context)
            .map(self._group)
            .reduceByKey(self._reduce)
            .map(self._finalize))


class JoinOperator(Operator):
  OPERATOR_TYPE = OperatorType.JOIN

  def __init__(self, left, right, condition, time_field, **kwargs):
    validate_getter(time_field)
    validate_condition(condition)

    self.filter_function = generate_filter(condition)
    self.left = Operator.parse(left)
    self.left_alias = left.get('alias') or 'left'
    self.right = Operator.parse(right)
    self.right_alias = right.get('alias') or 'right'
    self.time_field = time_field

  def _merge(self, events):
    event1, event2 = events
    event = {}
    for key, value in event1.iteritems():
      event['%s.%s' % (self.left_alias, key)] = value
    for key, value in event2.iteritems():
      event['%s.%s' % (self.right_alias, key)] = value
    timestamp = get_value(event, self.time_field)
    if timestamp is None:
      return None # No timestamp field calculated? Drop event.
    event[constants.TIMESTAMP_FIELD] = timestamp
    return event

  def _filter(self, event):
    if event is None:
      return False
    return self.filter_function(event)
  
  def get_rdd(self, spark_context):
    # TODO(usmanm): Use PyShark's `join` method here if its an equality join.
    return (self.left.get_rdd(spark_context)
            .cartesian(self.right.get_rdd(spark_context))
            .map(self._merge)
            .filter(self._filter))
