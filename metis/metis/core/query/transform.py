from metis.core.query import ExecutableNode
from metis.core.query.aggregate import Aggregator
from metis.core.query.aggregate import GroupBy
from metis.core.query.condition import Condition
from metis.core.query.stream import Stream
from metis.core.query.value import Value
from metis.utils.enum import Enum


def _parse_stream_or_transform(_dict):
  if not _dict:
    return None
  typ = _dict['type']
  if typ in Stream.Type.values():
    return Stream.parse(_dict)
  assert typ in Transform.Type.values()
  return Transform.parse(_dict)


class Transform(ExecutableNode):
  class Type(Enum):
    PROJECT = 'project'
    FILTER = 'filter'
    ORDER_BY = 'order_by'
    LIMIT = 'limit'
    AGGREGATE = 'aggregate'
    JOIN = 'join'

  def __init__(self, alias=None):
    self.alias = alias

  def validate(self):
    return self.type in Transform.Type.values()

  @classmethod
  def parse(self, _dict):
    typ = _dict['type']

    if typ in Stream.Type.values():
      return Stream.parse(_dict)

    assert typ in Transform.Type.values()
    del _dict['type']

    if 'stream' in _dict:
      _dict['stream'] = _parse_stream_or_transform(_dict['stream'])

    if typ == Transform.Type.PROJECT:
      return Project.parse(_dict)
    if typ == Transform.Type.FILTER:
      return Filter.parse(_dict)
    if typ == Transform.Type.ORDER_BY:
      return OrderBy.parse(_dict)
    if typ == Transform.Type.LIMIT:
      return Limit.parse(_dict)
    if typ == Transform.Type.AGGREGATE:
      return Aggregate.parse(_dict)
    if typ == Transform.Type.JOIN:
      return Join.parse(_dict)


class Project(Transform):
  def __init__(self, stream, fields, merge=False, **kwargs):
    self.type = Transform.Type.PROJECT
    self.stream = stream
    self.fields = fields
    self.merge = merge
    super(Project, self).__init__(**kwargs)

  @classmethod
  def parse(self, _dict, **kwargs):
    _dict['fields'] = map(Value.parse, _dict['fields'])
    return Project(**_dict)


class Filter(Transform):
  def __init__(self, stream, condition, **kwargs):
    self.type = Transform.Type.FILTER
    self.stream = stream
    self.condition = condition
    super(Filter, self).__init__(**kwargs)

  @classmethod
  def parse(self, _dict):
    _dict['condition'] = Condition.parse(_dict['condition'])
    return Filter(**_dict)


class OrderBy(Transform):
  def __init__(self, stream, fields, reverse=False, **kwargs):
    self.type = Transform.Type.ORDER_BY
    self.stream = stream
    self.fields = fields
    self.reverse = reverse
    super(OrderBy, self).__init__(**kwargs)

  @classmethod
  def parse(self, _dict):
    _dict['fields'] = map(Value.parse, _dict['fields'])
    return OrderBy(**_dict)


class Limit(Transform):
  def __init__(self, stream, limit, **kwargs):
    self.type = Transform.Type.LIMIT
    self.stream = stream
    self.limit = limit
    super(Limit, self).__init__(**kwargs)

  @classmethod
  def parse(self, _dict):
    return Limit(**_dict)


class Aggregate(Transform):
  def __init__(self, stream, group_by, aggregates, **kwargs):
    self.type = Transform.Type.AGGREGATE
    self.stream = stream
    self.aggregates = aggregates
    self.group_by = group_by
    super(Aggregate, self).__init__(**kwargs)
    
  @classmethod
  def parse(self, _dict):
    _dict['aggregates'] = map(Aggregator.parse, _dict['aggregates'])
    _dict['group_by'] = GroupBy.parse(_dict['group_by'])
    return Aggregate(**_dict)


class Join(Transform):
  def __init__(self, left, right, condition, **kwargs):
    self.type = Transform.Type.JOIN
    self.left = left
    self.right = right
    self.condition = condition
    super(Join, self).__init__(**kwargs)

  @classmethod
  def parse(self, _dict):
    _dict['left'] = _parse_stream_or_transform(_dict['left'])
    _dict['right'] = _parse_stream_or_transform(_dict['right'])
    _dict['condition'] = Condition.parse(_dict['condition'])
    return Join(**_dict)
