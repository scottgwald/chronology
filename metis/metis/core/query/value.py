import sys
import types

from metis.core.query import Node
from metis.utils.enum import Enum


class Value(Node):
  class Type(Enum):
    CONSTANT = 'constant'
    FUNCTION = 'function'
    PROPERTY = 'property'

  def __init__(self, alias=None):
    self.alias = alias

  @classmethod
  def parse(cls, _dict):
    typ = _dict.pop('type')
    assert typ in Value.Type.values()

    if typ == Value.Type.CONSTANT:
      return Constant(**_dict)
    if typ == Value.Type.FUNCTION:
      return Function.parse(_dict)
    if typ == Value.Type.PROPERTY:
      return Property.parse(_dict)


class Constant(Value):
  def __init__(self, value, **kwargs):
    self.type = Value.Type.CONSTANT
    self.value = value
    super(Constant, self).__init__(**kwargs)


class Function(Value):
  class Name(Enum):
    CEIL = 'ceil'
    FLOOR = 'floor'
    LOWERCASE = 'lowercase'
    UPPERCASE = 'uppercase'
    RANDOM_INT = 'rand_int'
    ADD = 'add'
    SUBTRACT = 'subtract'
    LEN = 'len'

  def __init__(self, arguments, **kwargs):
    self.type = Value.Type.FUNCTION
    self.arguments = arguments
    super(Function, self).__init__(**kwargs)

  @classmethod
  def parse(cls, _dict):
    _dict['arguments'] = map(Value.parse, _dict['arguments'])
    name = _dict.pop('name')
    assert name in Function.Name.values()
    return getattr(sys.modules[__name__], Enum.titlecase(name))(**_dict)

# Create classes for all Function.Name types.
for typ in Function.Name.values():
  class_name = Enum.titlecase(typ)
  typ = type(class_name, (Function, ), {'name': typ})
  setattr(sys.modules[__name__], class_name, typ)


class Property(Value):
  def __init__(self, name, default=None, **kwargs):
    self.type = Value.Type.PROPERTY
    self.name = name
    self.default = default
    super(Property, self).__init__(**kwargs)

  @classmethod
  def parse(cls, _dict):
    prop = Property(**_dict)
    assert isinstance(prop.name, types.StringTypes)
    return prop
