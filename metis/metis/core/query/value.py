import sys
import types

from metis.core.query import Node
from metis.utils.enum import Enum


def _get_cls(name):
  return getattr(sys.modules[__name__], name)


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

  def _combine_with(self, other, cls_name):
    if not isinstance(other, Value):
      raise TypeError

    alias = self.alias or other.alias
    cls = _get_cls(cls_name)

    if isinstance(self, cls):
      args = self.arguments[:]
      if isinstance(other, cls):
        args.extend(other.arguments)
        return cls(args, alias=alias)
      args.append(other)
      return cls(args, alias=alias)
    if isinstance(other, cls):
      args = [self]
      args.extend(other.arguments)
      return cls(args, alias=alias)
    return cls([self, other], alias=alias)

  def __add__(self, other):
    return self._combine_with(other, 'Add')

  def __sub__(self, other):
    return self._combine_with(other, 'Subtract')
  
  def __mul__(self, other):
    return self._combine_with(other, 'Multiply')

  def __div__(self, other):
    return self._combine_with(other, 'Divide')


class Constant(Value):
  def __init__(self, value, **kwargs):
    self.type = Value.Type.CONSTANT
    self.value = value
    super(Constant, self).__init__(**kwargs)

  def __repr__(self):
    return 'Constant(%s)' % self.value


class Function(Value):
  class Name(Enum):
    CEIL = 'ceil'
    FLOOR = 'floor'
    LOWERCASE = 'lowercase'
    UPPERCASE = 'uppercase'
    RANDOM_INT = 'rand_int'
    ADD = 'add'
    SUBTRACT = 'subtract'
    MULTIPLY = 'multiply'
    DIVIDE = 'divide'
    DATE_PART = 'date_part'
    DATE_TRUNC = 'date_trunc'
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

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__,
                       ', '.join(repr(arg) for arg in self.arguments))


class _DateUnit(Enum):
  SECOND = 'second'
  MINUTE = 'minute'
  HOUR = 'hour'
  DAY = 'day'
  MONTH = 'month'
  YEAR = 'year'


class DatePartUnit(_DateUnit):
  WEEK_DAY = 'week_day'


class DateTruncUnit(_DateUnit):
  WEEK = 'week'


# Create classes for all Function.Name types.
for typ in Function.Name.values():
  class_name = Enum.titlecase(typ)
  attrs = {'name': typ}
  if class_name == 'DateTrunc':
    attrs['Unit'] = DateTruncUnit
  elif class_name == 'DatePart':
    attrs['Unit'] = DatePartUnit
  typ = type(class_name, (Function, ), attrs)
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

  def __repr__(self):
    return 'Property(%s)' % self.name
