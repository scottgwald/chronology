import random
import re
import types

from common.event_tools import get_property
from metis.core.query.enums import (ConditionType,
                                    ConditionOpType,
                                    FunctionType,
                                    ValueType)

def _safe_function(func):
  '''
  Returns a wrapped function which returns the return value of `func` and `None`
  if an exception in raised in `func`.
  '''
  def wrapper(*args, **kwargs):
    try:
      return func(*args, **kwargs)
    except:
      return None
  return wrapper


# Functions.
def _get_function_args(event, args):
  if not args:
    return []
  extracted_args = []
  for arg in args:
    extracted_args.append(get_value(event, arg))
  return extracted_args

@_safe_function
def _ceil(value, base, *args):
  assert len(args) in (0, 1)
  if len(args):
    offset = args[0]
  else:
    offset = 0
  value = int(value) - offset
  return (value + (value % int(base))) + offset

@_safe_function
def _floor(value, base, *args):
  assert len(args) in (0, 1)
  if len(args):
    offset = args[0]
  else:
    offset = 0
  value = int(value) - offset
  return (value - (value % int(base))) + offset

@_safe_function
def _lowercase(value):
  return value.lower()

@_safe_function
def _uppercase(value):
  return value.upper()

@_safe_function
def _randint(low, high):
  return random.randint(low, high)

@_safe_function
def _add(*args):
  return sum(args)

@_safe_function
def _subtract(*args):
  if not args:
    return 0
  value = args[0]
  for arg in args[1:]:
    value -= arg
  return value

_len = _safe_function(len)
  
FUNCTIONS = {
  FunctionType.CEIL: _ceil,
  FunctionType.FLOOR: _floor,
  FunctionType.LOWERCASE: _lowercase,
  FunctionType.UPPERCASE: _uppercase,
  FunctionType.RANDOM_INT: _randint,
  FunctionType.ADD: _add,
  FunctionType.SUBTRACT: _subtract,
  FunctionType.LEN: _len
  }

# Ensure that we have a function mapped for each `FunctionType`.
assert FunctionType.values() == set(FUNCTIONS)


# Filters.
def _check_leaf_condition(event, condition):
  op = condition['op']
  if op == ConditionOpType.LT:
    result = (get_value(event, condition['left'])
              <
              get_value(event, condition['right']))
  elif op == ConditionOpType.LTE:
    result = (get_value(event, condition['left'])
              <=
              get_value(event, condition['right']))
  elif op == ConditionOpType.GT:
    result = (get_value(event, condition['left'])
              >
              get_value(event, condition['right']))
  elif op == ConditionOpType.GTE:
    result = (get_value(event, condition['left'])
              >=
              get_value(event, condition['right']))
  elif op == ConditionOpType.EQ:
    result = (get_value(event, condition['left'])
              ==
              get_value(event, condition['right']))
  elif op == ConditionOpType.CONTAINS:
    result = (get_value(event, condition['right'])
              in
              get_value(event, condition['left']))
  elif op == ConditionOpType.IN:
    result = (get_value(event, condition['left'])
              in
              get_value(event, condition['right']))
  elif op == ConditionOpType.REGEX:
    result = re.search(get_value(event, condition['right']),
                       get_value(event, condition['left']))
    result = bool(result)
  else:
    raise ValueError
  return result

def _check_condition(event, condition):
  if not 'conditions' in condition:
    result = _check_leaf_condition(event, condition)
  else:
    result = []
    for _condition in condition['conditions']:
      result.append(_check_condition(event, _condition))
    if condition['type'] == ConditionType.AND:
      result = all(result)
    elif condition['type'] == ConditionType.OR:
      result = any(result)
    else:
      raise ValueError
  result = bool(result)
  if bool(condition.get('not')):
    return not result
  return result
  
def generate_filter(condition):
  def _filter(event):
    if not event:
      return False
    return _check_condition(event, condition)
  return _filter


# Validators.
def _validate_constant(constant):
  assert constant['type'] == ValueType.CONSTANT
  assert 'value' in constant

def _validate_property(_property):
  assert _property['type'] == ValueType.PROPERTY
  assert isinstance(_property.get('name'), types.StringTypes)

def _validate_function(function):
  assert function['type'] == ValueType.FUNCTION
  assert isinstance(function.get('name'), types.StringTypes)
  args = function.get('args')
  assert isinstance(args, list)
  for arg in args:
    assert isinstance(arg, dict)
    arg_type = arg.get('type')
    assert arg_type in ValueType.values()
    if arg_type == ValueType.PROPERTY:
      _validate_property(arg)
    elif arg_type == ValueType.CONSTANT:
      _validate_constant(arg)
    elif arg_type == ValueType.FUNCTION:
      _validate_function(arg)

def validate_getter(getter):
  assert isinstance(getter, dict)
  getter_type = getter.get('type')
  assert getter_type in ValueType.values()
  if getter_type == ValueType.FUNCTION:
    assert getter.get('name') in FunctionType.values()
  if getter_type == ValueType.PROPERTY:
    _validate_property(getter)
  elif getter_type == ValueType.CONSTANT:
    _validate_constant(getter)
  elif getter_type == ValueType.FUNCTION:
    _validate_function(getter)
  else:
    raise ValueError

def validate_condition(condition):
  assert isinstance(condition, dict)
  keys = set(condition)
  keys.discard('not') # `not` key is optional.
  if keys == {'conditions', 'type'}:
    assert isinstance(condition['conditions'], list)
    assert condition.get('type') in ConditionType.values()
    for condition in condition['conditions']:
      validate_condition(condition)
  elif keys == {'left', 'right', 'op'}:
    assert condition.get('op') in ConditionOpType.values()
    validate_getter(condition.get('left'))
    validate_getter(condition.get('right'))
  else:
    raise ValueError

def get_value(event, getter):
  if getter['type'] == ValueType.CONSTANT:
    return getter['value']
  elif getter['type'] == ValueType.PROPERTY:
    try:
      return get_property(event, getter['name'])
    except KeyError:
      return getter.get('default')
  elif getter['type'] == ValueType.FUNCTION:
    args = _get_function_args(event, getter['args'])
    return FUNCTIONS[getter['name']](*args)

def get_property_names_from_getter(getter):
  properties = []
  if getter['type'] == ValueType.PROPERTY:
    properties.append(getter['name'])
  elif getter['type'] == ValueType.FUNCTION:
    for arg in getter['args']:
      properties.extend(get_property_names_from_getter(arg))
  return properties

class Counter(object):
  def __init__(self):
    self.counter = 0

  def increment(self):
    self.counter += 1
    return self.counter


def cast_to_number(value, default=None):
  for cast in (int, float):
    try:
      return cast(value)
    except ValueError:
      pass
  return default
