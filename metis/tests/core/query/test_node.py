import unittest

from copy import deepcopy

from metis.core.query.aggregate import Aggregator
from metis.core.query.aggregate import GroupBy
from metis.core.query.condition import Condition
from metis.core.query.stream import Stream
from metis.core.query.transform import Transform
from metis.core.query.value import Value


class NodeTestCase(unittest.TestCase):
  def test_parsing_and_to_dict(self):
    _property = {'type': 'property', 'name': 'x'}
    constant = {'type': 'constant', 'value': 1}
    function = {'type': 'function', 'name': 'add',
                'arguments': [deepcopy(_property), deepcopy(constant)]}
    self.assertEqual(Value.parse(deepcopy(constant)).to_dict(), constant)
    self.assertEqual(Value.parse(deepcopy(_property)).to_dict(), _property)
    self.assertEqual(Value.parse(deepcopy(function)).to_dict(), function)
    
    kronos = {'type': 'kronos', 'host': 'localhost', 'stream': 'mystream',
              'start_time': 100, 'end_time': 200}
    self.assertEqual(Stream.parse(deepcopy(kronos)).to_dict(), kronos)
    
    condition_lt = {'op': 'lt', 'left': deepcopy(_property),
                    'right': deepcopy(constant)}
    condition_eq = {'op': 'eq', 'left': deepcopy(function),
                    'right': deepcopy(_property)}
    condition_or = {'type': 'or', 'conditions': [deepcopy(condition_lt),
                                                 deepcopy(condition_eq)]}
    self.assertEqual(Condition.parse(deepcopy(condition_lt)).to_dict(),
                     condition_lt)
    self.assertEqual(Condition.parse(deepcopy(condition_eq)).to_dict(),
                     condition_eq)
    self.assertEqual(Condition.parse(deepcopy(condition_or)).to_dict(),
                     condition_or)
    
    avg = {'op': 'avg', 'arguments': [deepcopy(_property)], 'alias': 'myavg'}
    count = {'op': 'count', 'alias': 'mycount'}
    self.assertEqual(Aggregator.parse(deepcopy(avg)).to_dict(), avg)
    self.assertEqual(Aggregator.parse(deepcopy(count)).to_dict(), count)
    
    group_by = deepcopy(function)
    group_by['alias'] = 'mygroup'
    group_by = [group_by]
    self.assertEqual(GroupBy.parse(deepcopy(group_by)).to_dict(), group_by)
    
    project = {'type': 'project', 'fields': [deepcopy(_property)],
               'stream': deepcopy(kronos)}
    _filter = {'type': 'filter', 'condition': condition_lt,
              'stream': deepcopy(project)}
    aggregate = {'type': 'aggregate',
                 'group_by': deepcopy(group_by),
                 'aggregates': [deepcopy(avg), deepcopy(count)],
                 'stream': deepcopy(_filter)}
    join = {'type': 'join', 'left': deepcopy(aggregate),
            'right': deepcopy(project), 'condition': deepcopy(condition_or)}
    self.assertEqual(Transform.parse(deepcopy(project)).to_dict(), project)
    self.assertEqual(Transform.parse(deepcopy(_filter)).to_dict(), _filter)
    self.assertEqual(Transform.parse(deepcopy(aggregate)).to_dict(), aggregate)
    self.assertEqual(Transform.parse(deepcopy(join)).to_dict(), join)
