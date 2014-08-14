import datetime
import json
import requests
from metis.core.query.primitives import c, p, f, proj, filt, agg, agg_op
from metis.core.query.utils import _date_trunc, _date_part
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import kronos_time_to_datetime

def cpf(args):
  if args['cpf_type'] == 'constant':
    return c(args['value'])
  elif args['cpf_type'] == 'property':
    return p(args['name'])
  elif args['cpf_type'] == 'function':
    for i in range(len(args['args'])):
      args['args'][i] = cpf(args['args'][i])
    return f(args['name'], args['args'])
  else:
    raise ValueError("cpf_type must be constant, property, or function")

def transform(query_plan, args):
  fields = {} 
  fields[args[0]] = cpf(args[1])
  return proj(query_plan, fields, merge=True)

def filter(query_plan, args):
  condition = cond(cpf(args[0]['val']), cpf(args[2]['val']), args[1]['val'])
  return filt(query_plan, condition)

def aggregate(query_plan, args):
  aggregates = []
  arg = args[0]

  if arg['type'] == 'count':
    aggr = agg_op(arg['type'])
  else:
    aggr = agg_op(arg['type'], []) #TODO(derek): finish this line
  
  group = cpf(args[1])
  
  return agg(query_plan, {'@time':group}, [aggr])

def orderby(query_plan, args):
  fields = []
  for arg in args:
    fields.append(cpf(arg[0]))
  return order_by(query_plan, fields)

def limit(query_plan, args):
  return lim(query_plan, int(args[0]['val']))

def translate_query(query, stream, start_time, end_time):
  namespace = app.config['KRONOS_NAMESPACE']
  host = app.config['KRONOS_URL']

  query_plan = {
    'operator': 'kronos',
    'stream': stream,
    'start_time': start_time,
    'end_time': end_time,
    'namespace': namespace,
    'host': host,
  }

  operators = {
    'transform': transform,
    'filter': filter, #TODO(derek): fix this
    'aggregate': aggregate,
  }

  for instruction in query:
    operation = instruction['operation']
    operator = operation['operator']
    args = operation['args']
    query_plan = operators[operator](query_plan, args)

  return json.dumps({'plan': query_plan})

