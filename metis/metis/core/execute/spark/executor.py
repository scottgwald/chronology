# XXX(usmanm): PySpark in sensitive to modifying Python objects in functions
# like `map`. Please be wary of that! Using deepcopy everywhere will always
# work, but obviously is going to slow things down.

import json
import os
import re
import sys
import tempfile
import types
import zipfile

from copy import deepcopy

from metis import app
from metis.core.execute.base import Executor
from metis.core.execute.utils import cast_to_number
from metis.core.execute.utils import generate_filter
from metis.core.execute.utils import get_properties_accessed_by_value
from metis.core.execute.utils import get_value
from metis.core.query.aggregate import Aggregator
from metis.core.query.condition import Condition
from metis.core.query.stream import KronosStream
from metis.core.query.transform import Aggregate
from metis.core.query.transform import Filter
from metis.core.query.transform import Join
from metis.core.query.transform import Limit
from metis.core.query.transform import OrderBy
from metis.core.query.transform import Project

IGNORE_FILES_RE = re.compile('^.*\.pyc$', re.I)


def _copy_lib_for_spark_workers(file_path):
  zip_file = zipfile.ZipFile(file_path, 'w')
  # TODO(usmanm): Zip only the minimum set of files needed.
  for root, dirs, files in os.walk(app.config['PATH'], followlinks=True):
    for file_name in files:
      # Don't copy .pyc files to the lib file.
      if IGNORE_FILES_RE.match(file_name):
        continue
      zip_file.write(os.path.join(root, file_name),
                     os.path.join(root.replace(app.config['PATH'], 'metis'),
                                  file_name))
  zip_file.close()


def _setup_pyspark():
  # Set SPARK_HOME environment variable.
  os.putenv('SPARK_HOME', app.config['SPARK_HOME'])
  # From Python docs: Calling putenv() directly does not change os.environ, so 
  # it's better to modify os.environ. Also some platforms don't support
  # os.putenv. We'll just do both.
  os.environ['SPARK_HOME'] = app.config['SPARK_HOME']
  # Add PySpark to path.
  sys.path.append(os.path.join(app.config['SPARK_HOME'], 'python'))


class SparkExecutor(Executor):
  def __init__(self):
    # Setup PySpark. This is needed until PySpark becomes available on PyPI,
    # after which we can simply add it to requirements.txt.
    _setup_pyspark()
    from pyspark import SparkContext

    # Create a temporary .zip lib file for Metis, which will be copied over to
    # Spark workers so they can unpickle Metis functions and objects.
    metis_lib_file = tempfile.NamedTemporaryFile(suffix='.zip', delete=False)
    metis_lib_file.close()
    _copy_lib_for_spark_workers(metis_lib_file.name)
    
    # Also ship the Metis lib file so worker nodes can deserialize Metis
    # internal data structures.
    self.context = SparkContext(app.config['SPARK_MASTER'],
                                'Metis',
                                pyFiles=[metis_lib_file.name])

    # Delete temporary Metis lib file.
    os.unlink(metis_lib_file.name)

  def __getstate__(self):
    # Don't pickle the `SparkContext` object.
    state = self.__dict__.copy()
    del state['context']
    return state

  def finalize(self, rdd):
    return rdd.collect()

  def execute_kronos_stream(self, node):
    # TODO(usmanm): Read time slices of events in parallel from worker nodes.
    from pykronos import KronosClient

    client = KronosClient(node.host, blocking=True)
    events = client.get(node.stream,
                        node.start_time,
                        node.end_time,
                        namespace=node.namespace)
    return self.context.parallelize(events)

  def execute_aggregate(self, node):
    def group(event):
      # `key` can only be strings in Spark if you want to use `reduceByKey`.
      new_event = {value.alias: get_value(event, value)
                   for value in node.group_by.values}
      key = json.dumps(new_event, sort_keys=True)
      for aggregate in node.aggregates:
        arguments = aggregate.arguments
        if aggregate.op == Aggregator.Op.COUNT:
          if not len(arguments):
            value = 1
          else:
            value = 0 if get_value(event, arguments[0]) is None else 1
        elif aggregate.op == Aggregator.Op.SUM:
          value = cast_to_number(get_value(event, arguments[0]), 0)
        elif aggregate.op == Aggregator.Op.MIN:
          value = cast_to_number(get_value(event, arguments[0]), float('inf'))
        elif aggregate.op == Aggregator.Op.MAX:
          value = cast_to_number(get_value(event, arguments[0]), -float('inf'))
        elif aggregate.op == Aggregator.Op.AVG:
          value = cast_to_number(get_value(event, arguments[0]), None)
          if value is None:
            value = (0, 0)
          else:
            value = (value, 1)
        new_event[aggregate.alias] = value
      return (key, new_event)

    def _reduce(event1, event2):
      event = deepcopy(event1)
      for aggregate in node.aggregates:
        alias = aggregate.alias
        if aggregate.op in (Aggregator.Op.COUNT, Aggregator.Op.SUM):
          value = event1[alias] + event2[alias]
        elif aggregate.op == Aggregator.Op.MIN:
          value = min(event1[alias], event2[alias])
        elif aggregate.op == Aggregator.Op.MAX:
          value = max(event1[alias], event2[alias])
        elif aggregate.op == Aggregator.Op.AVG:
          value = (event1[alias][0] + event2[alias][0],
                   event1[alias][1] + event2[alias][1])
        event[alias] = value
      return event

    def finalize(event):
      # `event` is of the form (key, event).
      event = deepcopy(event[1])
      for aggregate in node.aggregates:
        if aggregate.op == Aggregator.Op.AVG:
          alias = aggregate.alias
          value = event[alias]
          if not value[1]:
            event[alias] = None
          else:
            event[alias] = value[0] / float(value[1])
      return event

    return (self.execute(node.stream)
            .map(group)
            .reduceByKey(_reduce)
            .map(finalize))

  def execute_filter(self, node):
    return self.execute(node.stream).filter(generate_filter(node.condition))

  def execute_join(self, node):
    left_alias = node.left.alias or 'left'
    right_alias = node.right.alias or 'right'

    def merge(events):
      event1, event2 = events
      if isinstance(event1, types.StringType):
        # Join case: events = (key, (event1, event2))
        event1, event2 = event2
        event = deepcopy(event1)
        event.update(event2)
      else:
        # Cartesian case: events = (event1, event2)
        event = {}
        for key, value in event1.iteritems():
          event['%s.%s' % (left_alias, key)] = value
        for key, value in event2.iteritems():
          event['%s.%s' % (right_alias, key)] = value
      return event

    def get_equijoin_key_values(condition):
      # condition must be a *leaf* condition.
      if getattr(condition, 'op', None) != Condition.Op.EQ:
        return None

      # Get properties being accessed by left and right side of the
      # conditional.
      left_properties = get_properties_accessed_by_value(condition.left)
      right_properties = get_properties_accessed_by_value(condition.right)

      if not (left_properties and right_properties):
        return None

      # Only return getters if both sides of the conditional read from different
      # streams. You can't use this optimization say if the condition is
      # (left.x + right.y = 10)
      # XXX: This isn't kosher for non-deterministic functions.
      if (all(p.startswith('%s.' % left_alias) for p in left_properties) and
          all(p.startswith('%s.' % right_alias) for p in right_properties)):
        return {'left': condition.left, 'right': condition.right}

      if (all(p.startswith('%s.' % right_alias) for p in left_properties) and
          all(p.startswith('%s.' % left_alias) for p in right_properties)):
        return {'left': condition.right, 'right': condition.left}

      return None

    def map_equijoin(alias, key_values):
      def map(event):
        new_event = {}
        for key, value in event.iteritems():
          new_event['%s.%s' % (alias, key)] = value
        key = json.dumps([get_value(new_event, value) for value in key_values])
        return (key, new_event)
      return map

    def setup_join():
      eq_join_key_values = []
      
      # TODO(usmanm): Right now we only optimize if the conditional is an EQ or
      # if its an AND and has some EQ in the top level. We don't do any
      # recursive searching in condition trees. Improve that.
      condition = node.condition
      _type = getattr(condition, 'type', None)
      if _type == Condition.Type.AND:
        filter_conditions = []
        for c in condition.conditions:
          values = get_equijoin_key_values(c)
          if values:
            eq_join_key_values.append(values)
          else:
            filter_conditions.append(c)
        if filter_conditions:
          condition.conditions = filter_conditions
        else:
          condition = None
      elif _type != Condition.Type.OR: # Ignore ORs for now.
        value = get_equijoin_key_values(condition)
        if value:
          eq_join_key_values.append(value)
          condition = None

      return eq_join_key_values, (generate_filter(condition)
                                  if condition else None)

    eq_join_key_values, filter_function = setup_join()

    if eq_join_key_values:
      mapped_left = (self.execute(node.left)
                     .map(map_equijoin(
                       left_alias,
                       [value['left'] for value in eq_join_key_values])))
      mapped_right = (self.execute(node.right)
                      .map(map_equijoin(
                        right_alias,
                        [value['right'] for value in eq_join_key_values])))
      joined = mapped_left.join(mapped_right).map(merge)
    else:
      # Naive O(n^2) cartesian product.
     joined = (self.execute(node.left).cartesian(self.execute(node.right))
               .map(merge))

    if filter_function:
      joined = joined.filter(filter_function)
    return joined

  def execute_limit(self, node):
    # TODO(usmanm): Is there a better way than to collect and redistribute all
    # events?
    return self.context.parallelize(self.execute(node.stream).take(node.limit))

  def execute_order_by(self, node):
    return (self.execute(node.stream)
            .keyBy(lambda e: tuple(get_value(e, field)
                                   for field in node.fields))
            .sortByKey(ascending=not node.reverse)
            .map(lambda e: e[1]))

  def execute_project(self, node):
    def project(event):
      if node.merge:
        new_event = deepcopy(event)
      else:
        new_event = {}
      for field in node.fields:
        new_event[field.alias] = get_value(event, field)
      return new_event
    return self.execute(node.stream).map(project)
