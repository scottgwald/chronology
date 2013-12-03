from metis.core.query.enums import (AggregateType,
                                    ConditionType,
                                    ConditionOpType,
                                    OperatorType,
                                    ValueType)


def kstream(stream, start_time, end_time, host, namespace=None):
  return {'operator': OperatorType.KRONOS,
          'stream': stream,
          'start_time': start_time,
          'end_time': end_time,
          'namespace': namespace,
          'host': host}

def p(name):
  return {'type': ValueType.PROPERTY, 'name': name}

def f(name, args):
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

def cond_or(conditions):
  return {'conditions': conditions,
          'type': ConditionType.OR}

def cond_and(conditions):
  return {'conditions': conditions,
          'type': ConditionType.AND}

def filt(stream, condition):
  return {'operator': OperatorType.FILTER,
          'stream': stream,
          'type': type,
          'condition': condition}

def agg_op(op, args=[], alias=None):
  assert op in AggregateType.values()
  return {'op': op, 'args': args, 'alias': alias}

def agg(stream, groups, aggregates):
  return {'operator': OperatorType.AGGREGATE,
          'stream': stream,
          'groups': groups,
          'aggregates': aggregates}

def s(stream, alias=None):
  return {'stream': s, 'alias': alias}

def join(left, right, condition, time_field):
  return {'operator': OperatorType.JOIN,
          'left': left,
          'right': right,
          'condition': condition,
          'time_field': time_field}
