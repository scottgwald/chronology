class IncrementingCounter(object):

  def __init__(self):
    self._counter = 0

  def another(self):
    self._counter += 1
    return self._counter


_counter = IncrementingCounter()
  

def p(name, alias=None):
  return {'type': 'property', 'name': name,
          'alias': alias if alias else name}


def f(name, args, alias=None):
  return {'type': 'function', 'name': name, 'args': args,
          'alias': alias if alias else name}


def c(value, alias=None):
  return {'type': 'constant', 'value': value,
          'alias': alias if alias else 'constant_{}'.format(
              _counter.another())}


def filt_or(conditions):
  return {'type': 'or',
          'conditions': conditions}


def filt_and(conditions):
  return {'type': 'and',
          'conditions': conditions}


def filt_cond(left, right, operand):
  return {'type': 'comparison',
          'left': left,
          'right': right,
          'op': operand}


def filt(conditions):
  return {'operator': 'FILTER',
          'conditions': conditions}


def aggop(operation, args, alias=None):
  return {'op': operation, 'args': args,
          'alias': alias if alias else 'operation_{}'.format(
              _counter.another())}

def agg(groups, aggregates):
  return {'operator': 'AGGREGATE',
          'groups': groups,
          'aggregates': aggregates}


def proj(properties):
  return {'operator': 'PROJECTION',
          'properties': properties}


def kstream(stream, start, end, server, namespace):
  return {'operator': 'KRONOS',
          'stream': stream,
          'start': start,
          'end': end,
          'namespace': namespace,
          'server': server}


def join(left, right, conditions, time_field):
  return {'operator': 'JOIN',
          'left': left,
          'right': right,
          'conditions': conditions,
          'time_field': time_field}


def jc(left, right, operand):
  return {'left': left, 'right': right, 'op': operand}


def jf(leftright, property, alias=None):
  return {'stream': leftright, 'property': property,
          'alias': alias if alias else property}

def s(stream, alias=None):
  return {'stream': stream,
          'alias': alias if alias else stream}
    
class JoinPath(object):
  LEFT = 'left'
  RIGHT = 'right'

class Comparison(object):
  LESS_THAN = 'lt'
  LESS_THAN_EQUAL = 'le'
  GREATER_THAN = 'gt'
  GREATER_THAN_EQUAL = 'ge'  
  EQUAL = 'eq'
  
TIME = '@time'
ID = '@id'
