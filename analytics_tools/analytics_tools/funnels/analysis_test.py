import datetime
import unittest

from mock import Mock
from mock import patch

from analysis import funnel_analyze
from analysis import FunnelStep
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import timedelta_to_kronos_time


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

  def get_stream5(self):
    start = datetime.datetime(2014,3,21)
    delta = datetime.timedelta(minutes=1)
    events = []
    for i in range(0, 20, 2):
      events.append({'userId': str(i),
                     'type': 'a' if i % 2 else 'b',
                     '@time': datetime_to_kronos_time(start)})
      start += delta
    return events

  def test_funnel(self):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream2()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step1 = FunnelStep('stream1')
    step2 = FunnelStep('stream2')
    funnel_output = funnel_analyze(client, [step1, step2],
                                  start, end,
                                  end, {}, None)
    self.assertEqual(funnel_output.stream_sizes(), [20, 7])

  def test_funnel_fuzzy_time(self):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream3()])

    fuzzy_time = datetime.timedelta(minutes=2)
    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step1 = FunnelStep('stream1')
    step2 = FunnelStep('stream2')
    funnel_output = funnel_analyze(client, [step1, step2],
                                  start, end,
                                  end, {}, None, fuzzy_time=fuzzy_time)
    self.assertEqual(funnel_output.stream_sizes(), [20, 1])

  def test_funnel_properties(self):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream3()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step1 = FunnelStep('stream1', output_fields=['type'])
    step2 = FunnelStep('stream2', output_fields=['type'])
    funnel_output = funnel_analyze(client, [step1, step2],
                                   start, end, end, {}, None)
    self.assertEqual(funnel_output.stream_sizes(), [20, 2])
    self.assertEqual(len(funnel_output.stream_data()[0]), 20)
    self.assertEqual(funnel_output.stream_data()[0]['0'], {'type': 'b'})
    self.assertEqual(funnel_output.stream_data()[0]['1'], {'type': 'a'})

  @patch('analysis.log')
  def test_funnel_properties_missing_field(self, mock_logging):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream3()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step1 = FunnelStep('stream1', output_fields=['type'])
    step2 = FunnelStep('stream2', output_fields=['color'])
    funnel_output = funnel_analyze(client, [step1, step2],
                                   start, end, end, {}, None)
    self.assertEqual(funnel_output.stream_sizes(), [20, 2])
    self.assertEqual(funnel_output.stream_data()[1]['0'], {})
    mock_logging.warn.assert_called_with(
        'Field %s does not appear in stream %s', 'color', 'stream2')

  def test_user_id_mapping(self):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream4(),
                                   self.get_stream4()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step1 = FunnelStep('stream1', output_fields=['type'])
    step2 = FunnelStep('stream2', user_field='username', output_fields=['type'])
    step3 = FunnelStep('stream3', user_field='username', output_fields=['type'])
    user_id_mappers = {'username': lambda x: x}
    funnel_output = funnel_analyze(client, [step1, step2, step3],
                                   start, end, end, user_id_mappers, None)
    self.assertEqual(funnel_output.stream_sizes(), [20, 2, 2])
    self.assertEqual(len(funnel_output.stream_data()[0]), 20)
    self.assertEqual(funnel_output.stream_data()[0]['0'], {'type': 'b'})
    self.assertEqual(funnel_output.stream_data()[0]['1'], {'type': 'a'})

  @patch('analysis.log')
  def test_user_id_mapping_missing(self, mock_logging):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream4()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step1 = FunnelStep('stream1')
    step2 = FunnelStep('stream2')
    with self.assertRaisesRegexp(UnboundLocalError,
                                 ("local variable 'user' referenced before "
                                  "assignment")):
        funnel_analyze(client, [step1, step2],
                       start, end, end, {}, None)
    mock_logging.error.assert_called_with(
        'Unable to get field %s on %s from %s', 'userId',
        'stream2',
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
    step1 = FunnelStep('stream1', event_filter=get_type_a,
                       output_fields=['type'])
    step2 = FunnelStep('stream2', event_filter=get_type_a,
                       output_fields=['type'])
    funnel_output = funnel_analyze(client, [step1, step2],
                                   start, end, end, {}, None)
    self.assertEqual(funnel_output.stream_sizes(), [10, 1])
    self.assertEqual(len(funnel_output.stream_data()[0]), 10)

  def test_user_filter(self):
    def get_odd_users(user):
      return int(user) % 2

    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream3()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step1 = FunnelStep('stream1', output_fields=['type'])
    step2 = FunnelStep('stream2', output_fields=['type'])
    funnel_output = funnel_analyze(client, [step1, step2],
                                   start, end, end, {}, get_odd_users)
    self.assertEqual(funnel_output.stream_sizes(), [10, 1])
    self.assertEqual(len(funnel_output.stream_data()[0]), 10)

  def test_stream_inversion(self):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream5()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step1 = FunnelStep('stream1', output_fields=['type'])
    step2 = FunnelStep('stream5', invert=True)
    funnel_output = funnel_analyze(client, [step1, step2],
                                   start, end, end, {}, None)
    self.assertEqual(funnel_output.stream_sizes(), [20, 10])
    # Second funnel step should only contain odd users because of inversion.
    self.assertIn('3', funnel_output.user_ids()[1])
    self.assertIn('5', funnel_output.user_ids()[1])

  def test_stream_inversion_timestamps(self):
    """
    Test that if stream is inverted, timestamp of last action is the
    timestamp of the last action in the previous funnel step.
    """
    from analysis import IdentityDict
    from analysis import _stream_earliest_action

    client = Mock()
    client.get = Mock(side_effect=[self.get_stream5()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step = FunnelStep('stream5', invert=True)
    last_user_action = {'0': datetime_to_kronos_time(start),
                        '1': datetime_to_kronos_time(start)}
    step_output = _stream_earliest_action(client, step,
                                          start, end,
                                          timedelta_to_kronos_time(
                                            datetime.timedelta(minutes=5)),
                                          last_user_action,
                                          {'userId': IdentityDict()})
    user_action = step_output['user_action']
    self.assertEqual(len(user_action), 1)
    self.assertEqual(user_action['1'], datetime_to_kronos_time(start))

  def test_stream_inversion_invert_in_first_step(self):
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream5()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step1 = FunnelStep('stream1', output_fields=['type'], invert=True)
    step2 = FunnelStep('stream5', output_fields=['type'], invert=True)
    with self.assertRaises(AssertionError):
      funnel_analyze(client, [step1, step2], start, end, end, {}, None)

  def test_stream_inversion_invert_and_output_fields_raises_assertion_error(self): # noqa
    client = Mock()
    client.get = Mock(side_effect=[self.get_stream1(),
                                   self.get_stream5()])

    start = datetime.datetime(2014,3,20)
    end = datetime.datetime(2014,3,21)
    step1 = FunnelStep('stream1', output_fields=['type'])
    step2 = FunnelStep('stream5', invert=True, output_fields=['type'])
    with self.assertRaises(AssertionError):
      funnel_analyze(client, [step1, step2], start, end, end, {}, None)
                                     
