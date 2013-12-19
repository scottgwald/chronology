from metis.core.query.enums import (AggregateType,
                                    ConditionType,
                                    ConditionOpType,
                                    OperatorType,
                                    ValueType)


TIME = '@time'
ID = '@id'


def kstream(stream, start_time, end_time, host, namespace=None):
  return {'operator': OperatorType.KRONOS,
          'stream': stream,
          'start_time': start_time,
          'end_time': end_time,
          'namespace': namespace,
          'host': host}

def p(name, alias=None):
  return {'type': ValueType.PROPERTY, 'name': name,
          'alias': alias if alias else name}


def f(name, args, alias=None):
  retval = {'type': ValueType.FUNCTION, 'name': name, 'args': args}
  if alias:
    retval['alias'] = alias
  return retval


def c(value, alias=None):
  retval = {'type': ValueType.CONSTANT, 'value': value}
  if alias:
    retval['alias'] = alias
  return retval


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


def cond_or(conditions):
  return {'conditions': conditions,
          'type': ConditionType.OR}


def cond_and(conditions):
  return {'conditions': conditions,
          'type': ConditionType.AND}


def filt(stream, condition):
  return {'operator': OperatorType.FILTER,
          'stream': stream,
          'condition': condition}


def agg_op(op, args=[], alias=None):
  assert op in AggregateType.values()
  retval = {'op': op, 'args': args}
  if alias:
    retval['alias'] = alias
  return retval


def agg(stream, groups, aggregates):
  return {'operator': OperatorType.AGGREGATE,
          'stream': stream,
          'groups': groups,
          'aggregates': aggregates}


def join(left, right, condition, time_field):
  return {'operator': OperatorType.JOIN,
          'left': left,
          'right': right,
          'condition': condition,
          'time_field': time_field}
