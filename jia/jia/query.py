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
  for arg in args:
    fields[arg['name']] = cpf(arg['value'])
  return proj(query_plan, fields)

def filter(query_plan, args):
  condition = cond(cpf(args['left']), cpf(args['right']), args['operator'])
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

def translate_query(query):
  stream = 'locu.claim_api.claim.wsb.new_user_signup'
  namespace = 'locu'
  host = 'http://cassandra1.locu.com:8153'

  now = datetime.datetime.now()
  start = now - datetime.timedelta(days=1)
  start_time = datetime_to_kronos_time(start)
  end_time = datetime_to_kronos_time(now)

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
    operator = instruction['operator']
    args = instruction['args']
    query_plan = operators[operator](query_plan, args)

  return json.dumps({'plan': query_plan})


q = translate_query([
  {
    'operator': 'aggregate',
    'args': [
      {
        'type': 'count',
      },
      {
        'cpf_type': 'function',
        'name': 'datetrunc',
        'args': [
          {
            'cpf_type': 'property',
            'name': '@time'
          },
          {
            'cpf_type': 'constant',
            'value': 'hour',
          }
        ]
      }
    ]
    # 'args': [
    #   {
    #     'name': '@time',
    #     'value': {'cpf_type': 'property', 'name': '@time'}
    #   },
    #   {
    #     'name': 'E-Mail',
    #     'value': {'cpf_type': 'property', 'name': 'email'}
    #   }
    # ]
  }
])

# r = requests.post("http://localhost:8155/1.0/query", data=q)
# print r.text

import pprint
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(json.loads(q))