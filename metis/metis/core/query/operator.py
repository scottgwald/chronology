import inspect
import json
import sys
import types

from metis.conf import constants
from metis.core.query.enums import (AggregateType,
                                    ConditionType,
                                    ConditionOpType,
                                    OperatorType)
from metis.core.query.utils import (cast_to_number,
                                    Counter,
                                    get_property_names_from_getter,
                                    get_value,
                                    generate_filter,
                                    validate_condition,
                                    validate_getter)

# XXX(usmanm): PySpark in sensitive to modifying Python objects in functions
# like `map`. Please be wary of that!

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
      projection = {}

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
    for group, getter in groups.iteritems():
      validate_getter(getter)
    counter = Counter()
    aliases = set()
    for aggregate in aggregates:
      args = aggregate.get('args', [])
      assert isinstance(args, list)
      for arg in args:
        validate_getter(arg)
      assert aggregate.get('op') in AggregateType.values()
      if not aggregate.get('alias'):
        aggregate['alias'] = '%s_%s' % (aggregate['op'], counter.increment())
      if aggregate['alias'] in aliases:
        raise ValueError
      aliases.add(aggregate['alias'])
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
    return (self.stream.get_rdd(spark_context)
            .map(self._group)
            .reduceByKey(self._reduce)
            .map(self._finalize))


class JoinOperator(Operator):
  OPERATOR_TYPE = OperatorType.JOIN

  def __init__(self, left, right, condition, **kwargs):
    validate_condition(condition)

    self.left = Operator.parse(left)
    self.left_alias = left.get('alias') or 'left'
    self.right = Operator.parse(right)
    self.right_alias = right.get('alias') or 'right'
    self._setup_join(condition)

  def _merge(self, events):
    event1, event2 = events
    if isinstance(event1, types.StringType):
      # Join case: events = (key, (event1, event2))
      event1, event2 = event2
      event = {}
      event.update(event1)
      event.update(event2)
    else:
      # Cartesian case: events = (event1, event2)
      event = {}
      for key, value in event1.iteritems():
        event['%s.%s' % (self.left_alias, key)] = value
      for key, value in event2.iteritems():
        event['%s.%s' % (self.right_alias, key)] = value
    return event

  def _get_equijoin_key_getters(self, condition):
    # condition must be a *leaf* condition.
    if condition.get('op') != ConditionOpType.EQ:
      return None

    # Get properties being accessed by left and right side of the
    # conditional.
    left_properties = get_property_names_from_getter(condition['left'])
    right_properties = get_property_names_from_getter(condition['right'])

    if not (left_properties and right_properties):
      return None

    # Only return getters if both sides of the conditional read from different
    # streams. You can't use this optimization say if the condition is
    # (left.x + right.y = 10)
    if (all(p.startswith('%s.' % self.left_alias)
            for p in left_properties) and
        all(p.startswith('%s.' % self.right_alias)
            for p in right_properties)):
      return {'left': condition['left'], 'right': condition['right']}

    if (all(p.startswith('%s.' % self.right_alias)
            for p in left_properties) and
        all(p.startswith('%s.' % self.left_alias)
            for p in right_properties)):
      return {'left': condition['right'], 'right': condition['left']}

    return None


  def _map_equijoin(self, alias, key_getters):
    def _map(event):
      new_event = {}
      for key, value in event.iteritems():
        new_event['%s.%s' % (alias, key)] = value
      key = json.dumps([get_value(new_event, getter)
                        for getter in key_getters])
      return (key, new_event)
    return _map

  def _setup_join(self, condition):
    eq_join_getters = []

    # TODO(usmanm): Right now we only optimize if the conditional is an EQ or
    # if its an AND and has some EQ in the top level. We don't do any recursive
    # searching in condition trees. Improve that.
    if condition.get('type') == ConditionType.AND:
      filter_conditions = []
      for _condition in condition['conditions']:
        getter = self._get_equijoin_key_getters(_condition)
        if getter:
          eq_join_getters.append(getter)
        else:
          filter_conditions.append(_condition)
      if filter_conditions:
        condition['conditions'] = filter_conditions
      else:
        condition = None
    elif condition.get('type') != ConditionType.OR: # Ignore ORs for now.
      getter = self._get_equijoin_key_getters(condition)
      if getter:
        eq_join_getters.append(getter)
        condition = None
    if eq_join_getters: # Did we find any getters to map to equijoins?
      self._optimize_equijoin = True
      self._eq_getters = eq_join_getters
    else:
      self._optimize_equijoin = False

    self._filter_function = generate_filter(condition) if condition else None

  def get_rdd(self, spark_context):
    if self._optimize_equijoin:
      mapped_left = (self.left.get_rdd(spark_context)
                     .map(self._map_equijoin(
                       self.left_alias,
                       [getter['left'] for getter in self._eq_getters])))
      mapped_right = (self.right.get_rdd(spark_context)
                      .map(self._map_equijoin(
                        self.right_alias,
                        [getter['right'] for getter in self._eq_getters])))
      joined = (mapped_left
                .join(mapped_right)
                .map(self._merge))
    else:
      # Naive O(n^2) cartesian product.
     joined =  (self.left.get_rdd(spark_context)
                .cartesian(self.right.get_rdd(spark_context))
                .map(self._merge))

    if self._filter_function:
      joined = joined.filter(self._filter_function)
    return joined
