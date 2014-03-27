import datetime
import unittest

from mock import Mock
from mock import patch

from analysis import funnel_analyze
from pykronos.utils.time import datetime_to_kronos_time

class TestFunnelAnalysis(unittest.TestCase):

  def get_stream1(self):
    start = datetime.datetime(2014,3,20)
    delta = datetime.timedelta(minutes=1)
    events = []
    for i in range(0, 20):
      events.append({'userId': str(i),
                     'type': 'a' if i % 2 else 'b',
                     '@time': datetime_to_kronos_time(start)})
      start += delta
    return events

  def get_stream2(self):
    start = datetime.datetime(2014,3,21)
    delta = datetime.timedelta(minutes=1)
    events = []
    for i in range(0, 20, 3):
      events.append({'userId': str(i),
                     'type': 'a' if i % 2 else 'b',
                     '@time': datetime_to_kronos_time(start)})
      start += delta
    return events

  def get_stream3(self):
    user_ids = ['0', '1']
    fuzzy_time = datetime.timedelta(minutes=2)
    times = [datetime.datetime(2014,3,20) - fuzzy_time,
             datetime.datetime(2014,3,20)]
    events = []
    for user_id, time in zip(user_ids, times):
      events.append({'userId': user_id,
                     'type': 'a',
                     '@time': datetime_to_kronos_time(time)})
    return events

  def get_stream4(self):
    user_ids = ['0', '1']
    fuzzy_time = datetime.timedelta(minutes=2)
    times = [datetime.datetime(2014,3,20) - fuzzy_time,
             datetime.datetime(2014,3,20)]
    events = []
    for user_id, time in zip(user_ids, times):
      events.append({'username': user_id,
                     'type': 'a',
                     '@time': datetime_to_kronos_time(time)})
    return events

  def test_funnel(self):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream2()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    stream_sizes = funnel_analyze(client, [('stream1', None, 'userId'),
                                           ('stream2', None, 'userId')],
                                  start, end,
                                  end, {}, None)
    self.assertEqual(stream_sizes, [20, 7])

  def test_funnel_fuzzy_time(self):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream3()])

    fuzzy_time = datetime.timedelta(minutes=2)
    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    stream_sizes = funnel_analyze(client, [('stream1', None, 'userId'),
                                           ('stream2', None, 'userId')],
                                  start, end,
                                  end, {}, None, fuzzy_time=fuzzy_time)
    self.assertEqual(stream_sizes, [20, 1])

  def test_funnel_properties(self):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream3()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    properties = [['type'],['type']]
    stream_sizes, stream_data = funnel_analyze(
        client, [('stream1', None, 'userId'), ('stream2', None, 'userId')],
        start, end, end, {}, None, output_properties=properties)
    self.assertEqual(stream_sizes, [20, 2])
    self.assertEqual(len(stream_data[0]), 20)
    self.assertEqual(stream_data[0]['0'], {'type': 'b'})
    self.assertEqual(stream_data[0]['1'], {'type': 'a'})

  @patch('analysis.log')
  def test_funnel_properties_missing_field(self, mock_logging):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream3()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    properties = [['type'],['color']]
    stream_sizes, stream_data = funnel_analyze(
        client, [('stream1', None, 'userId'), ('stream2', None, 'userId')],
        start, end, end, {}, None, output_properties=properties)
    self.assertEqual(stream_sizes, [20, 2])
    self.assertEqual(stream_data[1]['0'], {})
    mock_logging.warn.assert_called_with(
        'Field %s does not appear in stream %s', 'color', 'stream2')

  def test_user_id_mapping(self):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream4(),
                                   self.get_stream4()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    properties = [['type'],['type'],[]]
    user_id_mappers = {'username': lambda x: x}
    stream_sizes, stream_data = funnel_analyze(
        client, [('stream1', None, 'userId'), ('stream2', None, 'username'),
                 ('stream2', None, 'username')],
        start, end, end, user_id_mappers, None, output_properties=properties)
    self.assertEqual(stream_sizes, [20, 2, 2])
    self.assertEqual(len(stream_data[0]), 20)
    self.assertEqual(stream_data[0]['0'], {'type': 'b'})
    self.assertEqual(stream_data[0]['1'], {'type': 'a'})

  @patch('analysis.log')
  def test_user_id_mapping_missing(self, mock_logging):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream4()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    with self.assertRaisesRegexp(UnboundLocalError,
                                 ("local variable 'user' referenced before "
                                  "assignment")):
        funnel_analyze(
            client, [('stream1', None, 'userId'), ('stream2', None, 'userId')],
            start, end, end, {}, None)
    mock_logging.error.assert_called_with(
        'Unable to get field %s on %s from %s', 'userId',
        ('stream2', None, 'userId'),
        {'username': '0',
         'type': 'a',
         '@time': datetime_to_kronos_time(datetime.datetime(2014,3,19,23,58))})


  def test_event_filter(self):
    def get_type_a(event):
      return event['type'] == 'a'

    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream3()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    properties = [['type'],['type'],[]]
    stream_sizes, stream_data = funnel_analyze(
        client, [('stream1', get_type_a, 'userId'),
                 ('stream2', get_type_a, 'userId')],
        start, end, end, {}, None, output_properties=properties)
    self.assertEqual(stream_sizes, [10, 1])
    self.assertEqual(len(stream_data[0]), 10)

  def test_user_filter(self):
    def get_odd_users(user):
      return int(user) % 2

    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream3()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    properties = [['type'],['type'],[]]
    stream_sizes, stream_data = funnel_analyze(
        client, [('stream1', None, 'userId'),
                 ('stream2', None, 'userId')],
        start, end, end, {}, get_odd_users, output_properties=properties)
    self.assertEqual(stream_sizes, [10, 1])
    self.assertEqual(len(stream_data[0]), 10)
