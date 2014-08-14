from metis.core.query import Node
from metis.core.query.value import Value
from metis.utils.enum import Enum


class Condition(Node):
  class Op(Enum):
    LT = 'lt'
    LTE = 'lte'
    GT = 'gt'
    GTE ='gte'
    EQ = 'eq'
    CONTAINS = 'contains'
    IN = 'in'
    REGEX = 'regex'

  class Type(Enum):
    AND = 'and'
    OR = 'or'

  def __init__(self, op, left, right, negate=False):
    self.op = op
    self.left = left
    self.right = right
    self.negate = negate

  @classmethod
  def parse(cls, _dict):
    if 'type' in _dict:
      assert _dict['type'] in (Condition.Type.AND, Condition.Type.OR)
      if _dict['type'] == Condition.Type.AND:
        return AndCondition([Condition.parse(c) for c in _dict['conditions']])
      return OrCondition([Condition.parse(c) for c in _dict['conditions']])

    assert _dict.get('op') in Condition.Op.values()
    return Condition(_dict['op'],
                     Value.parse(_dict['left']),
                     Value.parse(_dict['right']))

  def _combine_with(self, other, cls):
    assert isinstance(other, Condition)
    conditions = []
    if isinstance(self, cls):
      conditions.extend(self.conditions)
      if isinstance(other, cls):
        conditions.extend(other.conditions)
      else:
        conditions.append(other)
    elif isinstance(other, cls):
      conditions.extend(other.conditions)
      conditions.append(self)
    else:
      conditions = [self, other]
    return cls(conditions)

  def __and__(self, other):
    return self._combine_with(other, AndCondition)
    
  def __or__(self, other):
    return self._combine_with(other, OrCondition)

  def invert(self):
    self.negate = not self.negate


class AndCondition(Condition):
  def __init__(self, conditions, negate=False):
    self.type = 'and'
    self.conditions = conditions
    self.negate = negate


class OrCondition(Condition):
  def __init__(self, conditions, negate=False):
    self.type = Condition.Type.OR
    self.conditions = conditions
    self.negate = negate
