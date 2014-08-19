import operator
import random
import re
import types

from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse

from metis.common.event_tools import get_property
from metis.common.time import kronos_time_to_datetime
from metis.common.time import datetime_to_kronos_time
from metis.core.query.condition import Condition
from metis.core.query.value import DatePart
from metis.core.query.value import DateTrunc
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


def _ceil(value, base, offset=0):
  value = int(value) - offset
  return (value + (value % int(base))) + offset


def _floor(value, base, offset=0):
  value = int(value) - offset
  return (value - (value % int(base))) + offset


def _date_trunc(value, timeframe):
  """
  A date flooring function.

  Returns the closest datetime to the current one that aligns to timeframe.
  For example, _date_trunc('2014-08-13 05:00:00', DateTrunc.Unit.MONTH)
  will return a Kronos time representing 2014-08-01 00:00:00.
  """
  if isinstance(value, types.StringTypes):
    value = parse(value)
    return_as_str = True
  else:
    value = kronos_time_to_datetime(value)
    return_as_str = False
  timeframes = {
    DateTrunc.Unit.SECOND: (lambda dt:
                            dt - timedelta(microseconds=dt.microsecond)),
    DateTrunc.Unit.MINUTE: (lambda dt:
                            dt - timedelta(seconds=dt.second,
                                           microseconds=dt.microsecond)),
    DateTrunc.Unit.HOUR: (lambda dt:
                          dt - timedelta(minutes=dt.minute,
                                         seconds=dt.second,
                                         microseconds=dt.microsecond)),
    DateTrunc.Unit.DAY: lambda dt: dt.date(),
    DateTrunc.Unit.WEEK: lambda dt: dt.date() - timedelta(days=dt.weekday()),
    DateTrunc.Unit.MONTH: lambda dt: datetime(dt.year, dt.month, 1),
    DateTrunc.Unit.YEAR: lambda dt: datetime(dt.year, 1, 1)
    }
  value = timeframes[timeframe](value)
  if return_as_str:
    return value.isoformat()
  return datetime_to_kronos_time(value)


def _date_part(value, part):
  """
  Returns a portion of a datetime.

  Returns the portion of a datetime represented by timeframe.
  For example, _date_part('2014-08-13 05:00:00', DatePart.Unit.WEEK_DAY)
  will return 2, for Wednesday.
  """
  if isinstance(value, types.StringTypes):
    value = parse(value)
  else:
    value = kronos_time_to_datetime(value)
  value = kronos_time_to_datetime(value)
  parts = {
    DatePart.Unit.SECOND: lambda dt: dt.second,
    DatePart.Unit.MINUTE: lambda dt: dt.minute,
    DatePart.Unit.HOUR: lambda dt: dt.hour,
    DatePart.Unit.DAY: lambda dt: dt.day,
    DatePart.Unit.MONTH: lambda dt: dt.month,
    DatePart.Unit.YEAR: lambda dt: dt.year,
    DatePart.Unit.WEEK_DAY: lambda dt: dt.weekday(),
    }
  result = parts[part](value)
  return result


FUNCTIONS = {
  Function.Name.CEIL: _ceil,
  Function.Name.FLOOR: _floor,
  Function.Name.LOWERCASE: lambda s: s.lower(),
  Function.Name.UPPERCASE: lambda s: s.upper(),
  Function.Name.RANDOM_INT: lambda low, high: random.randint(low, high),
  Function.Name.ADD: lambda *args: sum(args),
  Function.Name.SUBTRACT: lambda *args: reduce(operator.sub, args),
  Function.Name.MULTIPLY: lambda *args: reduce(operator.mul, args),
  Function.Name.DIVIDE: lambda *args: reduce(operator.truediv, args),
  Function.Name.DATE_TRUNC: _date_trunc,
  Function.Name.DATE_PART: _date_part,
  Function.Name.LEN: len
  }

# Ensure that we have a function mapped for each `Function.Name`.
assert Function.Name.values() == set(FUNCTIONS)


@_safe_function
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
