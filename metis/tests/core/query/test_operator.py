import json
import random

from collections import defaultdict

from metis.conf import constants
from metis.core.query.enums import (AggregateType,
                                    ConditionOpType,
                                    FunctionType)
from metis.core.query.primitives import (agg,
                                         agg_op,
                                         c,
                                         cond,
                                         cond_or,
                                         cond_and,
                                         f,
                                         filt,
                                         join,
                                         kstream,
                                         limit,
                                         order_by,
                                         p,
                                         proj)
from tests.server import MetisServerTestCase


class OperatorTestCase(MetisServerTestCase):
  '''
  Unit tests for all available `Operator` types.
  '''
  def validate_response(self, response):
    self.assertEqual(response.status_codes, 200)
    return map(json.loads, response.data.splitlines())

  def test_kronos(self):
    events = self.query(
      kstream('test_kronos',
              0,
              1000)
      )
    self.assertEqual(len(events), 0)
    for i in xrange(25):
      self.kronos_client.put({
        'test_kronos': [{constants.TIMESTAMP_FIELD: random.randint(0, 999)}]
        })
    events = self.query(
      kstream('test_kronos',
              0,
              1000)
      )
    self.assertEqual(len(events), 25)
    for event in events:
      self.assertTrue(event[constants.TIMESTAMP_FIELD] >= 0)
      self.assertTrue(event[constants.TIMESTAMP_FIELD] < 1000)

  def test_project(self):
    for i in xrange(25):
      self.kronos_client.put({
        'test_project': [{constants.TIMESTAMP_FIELD: random.randint(0, 999),
                          'i': i,
                          'i+1': i+1}]
        })
    events = self.query(
      proj(kstream('test_project',
                   0,
                   1000),
           {'I': p('i'),
            'const': c(10),
            'func': f(FunctionType.ADD, [p('i'), p('i+1'), c(5)])},
           merge=True)
      )
    self.assertEqual(len(events), 25)
    for event in events:
      self.assertEqual(event['i'], event['I'])
      self.assertEqual(event['const'], 10)
      self.assertEqual(event['func'], event['i'] * 2 + 6)
      self.assertEqual(event['i+1'], event['i'] + 1)
      self.assertTrue(event[constants.TIMESTAMP_FIELD] >= 0)
      self.assertTrue(event[constants.TIMESTAMP_FIELD] < 1000)
    
  def test_filter(self):
    for i in xrange(2000):
      event = {constants.TIMESTAMP_FIELD: random.randint(0, 999),
                          'a': random.randint(0, 10),
                          'b': random.randint(50, 150),
                          'c': [random.randint(0, 20) for j in xrange(10)]}
      if random.randint(0, 100) > 50:
        event['d'] = 'iamlolcat'
      else:
        event['d'] = 'helloworld'
      self.kronos_client.put({'test_filter': [event]})
    events = self.query(
      filt(kstream('test_filter',
                   0,
                   1000),
           cond_and(cond_or(cond(p(constants.TIMESTAMP_FIELD),
                                 c(500),
                                 ConditionOpType.GT),
                            cond(p('b'), c(100), ConditionOpType.LTE)),
                    cond_or(cond(p('c'), p('a'), ConditionOpType.CONTAINS),
                            cond(p('d'), c('lolcat'), ConditionOpType.REGEX)))
           )
      )
    self.assertTrue(len(events) > 0)
    self.assertTrue(len(events) < 2000)
    for event in events:
      self.assertTrue(event[constants.TIMESTAMP_FIELD] > 500 or
                      event['b'] <= 100)
      self.assertTrue(event['a'] in event['c'] or
                      'lolcat' in event['d'])
      self.assertTrue(event[constants.TIMESTAMP_FIELD] >= 0)
      self.assertTrue(event[constants.TIMESTAMP_FIELD] < 1000)

  def test_order_by(self):
    for i in xrange(100):
      self.kronos_client.put({
        'test_order_by': [{constants.TIMESTAMP_FIELD: random.randint(0, 999),
                           'a': random.randint(0, 5),
                           'b': random.randint(1000, 10000)}]
        })
    events = self.query(
      # NOP projection to ensure events flow through Spark.
      proj(kstream('test_order_by',
                   0,
                   1000),
           {},
           merge=True)
      )
    # By default, should be ordered by time.
    self.assertEqual(len(events), 100)
    times = [event[constants.TIMESTAMP_FIELD] for event in events]
    self.assertEqual(times, sorted(times))

    events = self.query(
      order_by(kstream('test_order_by',
                       0,
                       1000),
               [p('a'), p('b')])
      )
    self.assertEqual(len(events), 100)
    a = b = -float('inf')
    for event in events:
      if a != event['a']:
        b = -float('inf')
      self.assertTrue(a <= event['a'])
      self.assertTrue(b <= event['b'])
      a = event['a']
      b = event['b']

  def test_limit(self):
    for i in xrange(20):
      self.kronos_client.put({
        'test_limit': [{constants.TIMESTAMP_FIELD: i}]
        })
    events = self.query(
      limit(kstream('test_limit',
                    0,
                    20),
            10)
      )
    self.assertEqual(len(events), 10)
    for i, event in enumerate(events):
      self.assertEqual(event[constants.TIMESTAMP_FIELD], i)

  def test_aggregate(self):
    sums = defaultdict(int)
    for i in xrange(200):
      a = random.randint(0, 2)
      self.kronos_client.put({
        'test_aggregate': [{constants.TIMESTAMP_FIELD: i,
                            'a': a}]
        })
      sums[50 * (i / 50)] += a
    events = self.query(
      agg(proj(kstream('test_aggregate',
                       0,
                       1000),
               {constants.TIMESTAMP_FIELD: f(FunctionType.FLOOR,
                                             [p(constants.TIMESTAMP_FIELD),
                                              c(50)])},
               merge=True
               ),
          {constants.TIMESTAMP_FIELD: p(constants.TIMESTAMP_FIELD)},
          [agg_op(AggregateType.COUNT, alias='count'),
           agg_op(AggregateType.SUM, [p('a')], alias='sum'),
           agg_op(AggregateType.MIN, [p('a')], alias='min'),
           agg_op(AggregateType.MAX, [p('a')], alias='max'),
           agg_op(AggregateType.AVG, [p('a')], alias='avg'),
           agg_op(AggregateType.VALUE_COUNT, [p('a')], alias='value_count')]
          )
      )
    self.assertEqual(len(events), 200 / 50)
    for event in events:
      self.assertEqual(event[constants.TIMESTAMP_FIELD] % 50, 0)
      self.assertEqual(event['count'], 50)
      self.assertEqual(event['min'], 0)
      self.assertEqual(event['max'], 2)
      self.assertEqual(event['sum'], sums[event[constants.TIMESTAMP_FIELD]])
      self.assertTrue(event['avg'] * 50 > event['sum'] - 0.1)
      self.assertTrue(event['avg'] * 50 < event['sum'] + 0.1)
      self.assertEqual(set(event['value_count']), {'0', '1', '2'})
      self.assertTrue(event['value_count']['0'] > 0)
      self.assertTrue(event['value_count']['0'] < 50)
      self.assertTrue(event['value_count']['1'] > 0)
      self.assertTrue(event['value_count']['1'] < 50)
      self.assertTrue(event['value_count']['2'] > 0)
      self.assertTrue(event['value_count']['2'] < 50)
      self.assertEqual(sum(event['value_count'].values()), 50)

    events = self.query(
      agg(kstream('test_aggregate',
                       0,
                       1000),
          {constants.TIMESTAMP_FIELD: f(FunctionType.FLOOR,
                                        [p(constants.TIMESTAMP_FIELD),
                                         c(50)])},
          [agg_op(AggregateType.COUNT, alias='count')]
          )
      )
    self.assertEqual(len(events), 200 / 50)

  def test_join(self):
    for i in xrange(100):
      self.kronos_client.put({
        'test_join1': [{constants.TIMESTAMP_FIELD: i,
                        'a': random.randint(0, 2),
                        'b': random.randint(0, 5)}]
        })
    for i in xrange(100):
      self.kronos_client.put({
        'test_join2': [{constants.TIMESTAMP_FIELD: i,
                        'a': random.randint(0, 2),
                        'b': random.randint(0, 5)}]
        })
    events = self.query(
      join(kstream('test_join1',
                   0,
                   200),
           kstream('test_join2',
                   0,
                   200),
           cond_and(cond(p('j1.a'), p('right.a'), ConditionOpType.EQ),
                    cond(p('j1.b'), p('right.b'), ConditionOpType.GT)),
           p('j1.%s' % constants.TIMESTAMP_FIELD),
           left_alias='j1')
      )
    self.assertTrue(len(events) > 0)
    for event in events:
      self.assertEqual(event[constants.TIMESTAMP_FIELD], 
                       event['j1.%s' % constants.TIMESTAMP_FIELD])
      self.assertEqual(event['j1.a'], event['right.a'])
      self.assertTrue(event['j1.b'] > event['right.b'])
      self.assertEqual(set(event),
                       {constants.TIMESTAMP_FIELD,
                        'j1.%s' % constants.TIMESTAMP_FIELD,
                        'right.%s' % constants.TIMESTAMP_FIELD,
                        'j1.%s' % constants.ID_FIELD,
                        'right.%s' % constants.ID_FIELD,
                        'j1.a', 'right.a',
                        'j1.b', 'right.b'})

  def test_join_eq(self):
    for i in xrange(200):
      self.kronos_client.put({
        'test_join_eq1': [{constants.TIMESTAMP_FIELD: random.randint(0, 999),
                        'a': i,
                        'b': i + 1}]
        })
    for i in xrange(200):
      self.kronos_client.put({
        'test_join_eq2': [{constants.TIMESTAMP_FIELD: random.randint(0, 999),
                        'a': i + 1,
                        'b': i + 2}]
        })

    # 1-1 join with property.
    events = self.query(
      join(kstream('test_join_eq1',
                   0,
                   1000),
           kstream('test_join_eq2',
                   0,
                   1000),
           # left.a == right.b
           cond(p('left.b'), p('right.a'), ConditionOpType.EQ),
           p('left.%s' % constants.TIMESTAMP_FIELD))
      )
    self.assertEqual(len(events), 200)
    for event in events:
      self.assertEqual(event['left.b'], event['right.a'])

    # 1-1 join with function.
    events = self.query(
      join(kstream('test_join_eq1',
                   0,
                   1000),
           kstream('test_join_eq2',
                   0,
                   1000),
           # left.a == (right.a - 1)
           cond(p('left.a'),
                f(FunctionType.SUBTRACT,
                  [p('right.a'), c(1)]),
                ConditionOpType.EQ),
           p('left.%s' % constants.TIMESTAMP_FIELD))
      )
    self.assertEqual(len(events), 200)
    for event in events:
      self.assertEqual(event['left.a'], event['right.a'] - 1)

    # 1-1 eqjoin with filtering.
    events = self.query(
      join(kstream('test_join_eq1',
                   0,
                   1000),
           kstream('test_join_eq2',
                   0,
                   1000),
           cond_and(cond(p('left.b'), p('right.a'), ConditionOpType.EQ),
                    cond(p('left.%s' % constants.TIMESTAMP_FIELD),
                         f(FunctionType.ADD,
                           [p('right.%s' % constants.TIMESTAMP_FIELD),
                            c(10)]),
                         ConditionOpType.GT)),
           p('left.%s' % constants.TIMESTAMP_FIELD))
      )
    self.assertTrue(len(events) > 0)
    self.assertTrue(len(events) < 200)
    for event in events:
      self.assertEqual(event['left.b'], event['right.a'])
      self.assertTrue(event['left.%s' % constants.TIMESTAMP_FIELD] >
                      event['right.%s' % constants.TIMESTAMP_FIELD] + 10)
