import sys

from metis.core.query import Node
from metis.core.query.value import Value
from metis.utils.enum import Enum


class Aggregator(Node):
  class Op(Enum):
    MIN = 'min'
    MAX = 'max'
    AVG = 'avg'
    COUNT = 'count'
    SUM = 'sum'

  def __init__(self, arguments, alias):
    self.arguments = arguments
    self.alias = alias
  
  @classmethod
  def parse(cls, _dict):
    op = _dict.pop('op')
    assert op in Aggregator.Op.values()

    _dict['arguments'] = map(Value.parse, _dict.get('arguments', []))

    if op in (Aggregator.Op.MIN, Aggregator.Op.MAX,
              Aggregator.Op.AVG, Aggregator.Op.SUM):
      assert len(_dict['arguments']) == 1
    if op == Aggregator.Op.COUNT:
      assert len(_dict['arguments']) in (0, 1)

    return getattr(sys.modules[__name__], Enum.titlecase(op))(**_dict)

# Create classes for all Aggregator.Op types.
for op in Aggregator.Op.values():
  class_name = Enum.titlecase(op)
  typ = type(class_name, (Aggregator, ), {'op': op})
  setattr(sys.modules[__name__], class_name, typ)


class GroupBy(Node):
  def __init__(self, values):
    if not isinstance(values, list):
      values = [values]
    self.values = values

  def to_dict(self):
    return map(lambda v: v.to_dict(), self.values)

  @classmethod
  def parse(cls, _list):
    if not isinstance(_list, list):
      _list = [_list]
    return GroupBy([Value.parse(v) for v in _list])
