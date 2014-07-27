from metis import app

from metis.core.query.enums import (AggregateType,
                                    ConditionType,
                                    ConditionOpType,
                                    FunctionType,
                                    OperatorType,
                                    ValueType)


def kstream(stream, start_time, end_time, host=app.config['KRONOS_SERVER'],
            namespace=None):
  return {'operator': OperatorType.KRONOS,
          'stream': stream,
          'start_time': start_time,
          'end_time': end_time,
          'namespace': namespace,
          'host': host}

def p(name, default=None):
  return {'type': ValueType.PROPERTY, 'name': name, 'default': default}


def f(name, args):
  assert name in FunctionType.values()
  return {'type': ValueType.FUNCTION, 'name': name, 'args': args}


def c(value):
  return {'type': ValueType.CONSTANT, 'value': value}


def proj(stream, fields, merge=False):
  return {'operator': OperatorType.PROJECT,
          'stream': stream,
          'merge': merge,
          'fields': fields}


def cond(left, right, operand):
  assert operand in ConditionOpType.values()
  return {'left': left,
          'right': right,
          'op': operand}


def cond_or(*conditions):
  return {'conditions': conditions,
          'type': ConditionType.OR}


def cond_and(*conditions):
  return {'conditions': conditions,
          'type': ConditionType.AND}


def filt(stream, condition):
  return {'operator': OperatorType.FILTER,
          'stream': stream,
          'condition': condition}


def agg_op(op, args=[], alias=None):
  assert op in AggregateType.values()
  return {'op': op,
          'args': args,
          'alias': alias}


def agg(stream, groups, aggregates):
  """
  :param stream: Metis stream
  :param groups: a dictionary of name, field pairs to group by.
    Ex: {TIMESTAMP_FIELD: p(TIMESTAMP_FIELD), 'state': p('state')}
  :param aggregates: a list of dictionaries returned by agg_op()
  """
  return {'operator': OperatorType.AGGREGATE,
          'stream': stream,
          'groups': groups,
          'aggregates': aggregates}

def join(left, right, condition, left_alias=None, right_alias=None):
  left = left.copy()
  left['alias'] = left_alias
  right = right.copy()
  right['alias'] = right_alias
  return {'operator': OperatorType.JOIN,
          'left': left,
          'right': right,
          'condition': condition}


def order_by(stream, fields, reverse=False):
  """
  :param stream: Metis stream
  :param fields: a list of fields to order by.
    Ex: [p(TIMESTAMP_FIELD), p(ID)]
  :param reverse: boolean, set to True if order should be reversed.
  """
  return {'operator': OperatorType.ORDER_BY,
          'stream': stream,
          'fields': fields,
          'reverse': reverse}


def limit(stream, limit):
  return {'operator': OperatorType.LIMIT,
          'stream': stream,
          'limit': limit}
