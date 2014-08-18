import operator
import random
import re

from metis.common.event_tools import get_property
from metis.core.query.condition import Condition
from metis.core.query.value import Function
from metis.core.query.value import Value


def _safe_function(func):
  '''
  Returns a wrapped function which returns the return value of `func` and `None`
  if an exception in raised in `func`.
  '''
  def wrapper(*args, **kwargs):
    try:
      return func(*args, **kwargs)
    except:
      # TODO(usmanm): Log here.
      return None
  return wrapper


# Functions.
def _get_function_args(event, args):
  if not args:
    return []
  arg_values = []
  for arg in args:
    arg_values.append(get_value(event, arg))
  return arg_values


@_safe_function
def _ceil(value, base, *args):
  assert len(args) <= 1
  offset = args[0] if len(args) else 0
  value = int(value) - offset
  return (value + (value % int(base))) + offset


@_safe_function
def _floor(value, base, *args):
  assert len(args) <= 1
  offset = args[0] if len(args) else 0
  value = int(value) - offset
  return (value - (value % int(base))) + offset


@_safe_function
def _subtract(*args):
  if not args:
    return 0
  value = args[0]
  for arg in args[1:]:
    value -= arg
  return value

  
FUNCTIONS = {
  Function.Name.CEIL: _ceil,
  Function.Name.FLOOR: _floor,
  Function.Name.LOWERCASE: _safe_function(lambda s: s.lower()),
  Function.Name.UPPERCASE: _safe_function(lambda s: s.upper()),
  Function.Name.RANDOM_INT: lambda low, high: random.randint(low, high),
  Function.Name.ADD: lambda *args: sum(args),
  Function.Name.SUBTRACT: _subtract,
  Function.Name.LEN: _safe_function(len)
  }

# Ensure that we have a function mapped for each `Function.Name`.
assert Function.Name.values() == set(FUNCTIONS)


def get_value(event, value):
  if value.type == Value.Type.CONSTANT:
    return value.value
  elif value.type == Value.Type.PROPERTY:
    try:
      return get_property(event, value.name)
    except KeyError:
      return value.default
  elif value.type == Value.Type.FUNCTION:
    args = _get_function_args(event, value.arguments)
    return FUNCTIONS[value.name](*args)


CONDITIONS = {
  Condition.Op.LT: operator.lt,
  Condition.Op.LTE: operator.le,
  Condition.Op.GT: operator.gt,
  Condition.Op.GTE: operator.ge,
  Condition.Op.EQ: operator.eq,
  Condition.Op.CONTAINS: lambda left, right: right in left,
  Condition.Op.IN: lambda left, right: left in right,
  Condition.Op.REGEX: lambda left, right: re.search(right, left)
  }

# Ensure that we have a function mapped for each `Condition.Op`.
assert Condition.Op.values() == set(CONDITIONS)


def _check_leaf_condition(event, condition):
  op = CONDITIONS[condition.op]
  left = get_value(event, condition.left)
  right = get_value(event, condition.right)
  return bool(op(left, right))


@_safe_function
def _check_condition(event, condition):
  if not hasattr(condition, 'conditions'):
    result = _check_leaf_condition(event, condition)
  else:
    result = []
    for c in condition.conditions:
      result.append(_check_condition(event, c))
    if condition.type == Condition.Type.AND:
      result = all(result)
    else:
      result = any(result)
  result = bool(result)
  if bool(condition.negate):
    return not result
  return result


def generate_filter(condition):
  def _filter(event):
    if not event:
      return False
    return _check_condition(event, condition)
  return _filter


def get_properties_accessed_by_value(value):
  properties = []
  if value.type == Value.Type.PROPERTY:
    properties.append(value.name)
  elif value.type == Value.Type.FUNCTION:
    for arg in value.arguments:
      properties.extend(get_properties_accessed_by_value(arg))
  return properties


def cast_to_number(value, default=None):
  for cast in (int, float):
    try:
      return cast(value)
    except (TypeError, ValueError):
      pass
  return default
