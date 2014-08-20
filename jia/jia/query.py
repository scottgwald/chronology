import datetime
import json
import sys
import metis.core.query.aggregate
import metis.core.query.value
from jia import app
from metis.core.query.aggregate import Aggregator, GroupBy
from metis.core.query.stream import KronosStream
from metis.core.query.transform import Project, Filter, Aggregate, OrderBy
from metis.core.query.transform import Limit
from metis.core.query.value import Constant, Property, Function
from metis.utils.enum import Enum
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import kronos_time_to_datetime


def cpf(args):
  print 'cpf args', args
  if args['cpf_type'] == 'constant':
    return Constant(args['constant_value'])
  elif args['cpf_type'] == 'property':
    return Property(args['property_name'])
  elif args['cpf_type'] == 'function':
    for i in range(len(args['function_args'])):
      args['function_args'][i] = cpf(args['function_args'][i])
    module = metis.core.query.value
    func = Enum.titlecase(args['function_name'])
    func_args = args['function_args']
    return getattr(module, func)(func_args)
  else:
    raise ValueError("cpf_type must be constant, property, or function")


def transform(query_plan, args):
  fields = {} 
  fields[args[0]] = cpf(args[1])
  return Project(query_plan, fields, merge=True)


def filter(query_plan, args):
  condition = cond(cpf(args[0]['val']), cpf(args[2]['val']), args[1]['val'])
  return Filter(query_plan, condition)


def agg_op(agg_type, agg_on, store_in):
  module = metis.core.query.aggregate
  op = Enum.titlecase(agg_type)
  getattr(module, op)([agg_on], alias=store_in)


def aggregate(query_plan, args):
  aggregates = []

  for agg in args[0]:
    aggregates.append(agg_op(agg[0]['val'], cpf(agg[1]['val']), agg[2]['val']))
  
  groups = []
  for group in args[1]:
    groups.append(cpf(group[0]['val']))

  group_by = GroupBy(groups)

  return Aggregate(query_plan, group_by, aggregates) 


def orderby(query_plan, args):
  fields = []
  for arg in args:
    fields.append(cpf(arg[0]))
  return OrderBy(query_plan, fields)


def limit(query_plan, args):
  return Limit(query_plan, int(args[0]['val']))


def translate_query(query, stream_name, start_time, end_time):
  host = app.config['KRONOS_URL']
  query_plan = KronosStream(host, stream_name, start_time, end_time)
  print start_time, end_time
  operators = {
    'transform': transform,
    'filter': filter,
    'aggregate': aggregate,
    'orderby': orderby,
    'limit': limit,
  }

  for instruction in query:
    operation = instruction['operation']
    operator = operation['operator']
    args = operation['args']
    query_plan = operators[operator](query_plan, args)

  return json.dumps({'plan': query_plan.to_dict()})

