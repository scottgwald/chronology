from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from dateutil.tz import tzutc
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import kronos_time_to_datetime
from pykronos.utils.time import kronos_time_to_epoch_time
from pykronos.utils.time import epoch_time_to_kronos_time

from metis import app
from metis.conf.constants import TIMESTAMP_FIELD
from metis.core.query.enums import AggregateType
from metis.core.query.enums import ConditionOpType
from metis.core.query.enums import FunctionType
from metis.core.query.primitives import agg
from metis.core.query.primitives import agg_op
from metis.core.query.primitives import c
from metis.core.query.primitives import cond
from metis.core.query.primitives import cond_and
from metis.core.query.primitives import f
from metis.core.query.primitives import join
from metis.core.query.primitives import kstream
from metis.core.query.primitives import p
from metis.core.query.primitives import proj


class DateUnit(object):
  DAYS = 'days'
  WEEKS = 'weeks'

  @staticmethod
  def round_date_down(the_date, unit):
    if isinstance(the_date, datetime):
      the_date = the_date.date()
    if unit == DateUnit.WEEKS:
      return the_date - timedelta(days=the_date.weekday())
    return the_date

  @staticmethod
  def unit_to_kronos_time(unit):
    if unit == DateUnit.DAYS:
      return epoch_time_to_kronos_time(timedelta(days=1).total_seconds())
    elif unit == DateUnit.WEEKS:
      return epoch_time_to_kronos_time(timedelta(weeks=1).total_seconds())
    return unit


def _date_to_datetime(d):
  return datetime(d.year, d.month, d.day).replace(tzinfo=tzutc())


def _cohort_stream_transform(kronos_url, stream, start, end,
                             transform, grouping_key, unit):
  start_stream = kstream(stream, start, end, kronos_url)
  if transform:
    transformed = transform(start_stream)
  else:
    transformed = start_stream
  projected = proj(
    transformed,
    {TIMESTAMP_FIELD: p(TIMESTAMP_FIELD),
     grouping_key: p(grouping_key),
     'date': f(FunctionType.FLOOR,
               [p(TIMESTAMP_FIELD),
                c(DateUnit.unit_to_kronos_time(unit)),
                c(start)])})
  # This leaves us with a single event per (user, unit time) pair.
  aggregated = agg(
    projected,
    {grouping_key: p(grouping_key),
     'date': p('date')},
    # The first time the user performed the event in that bucket.
    [agg_op(AggregateType.MIN, [p(TIMESTAMP_FIELD)], alias=TIMESTAMP_FIELD)])
  return aggregated


def cohort_queryplan(plan):
  """
  Input:
  {
   'kronos_url': 'http://...',
   'cohort':
    {'stream': CohortTest.EMAIL_STREAM, # Kronos stream to define cohort from.
     'transform': lambda x: x,          # Transformations on the kstream.
     'start': date.now(),               # The day of the first cohort.
     'unit': DateUnit.XX,               # Users are in the same cohort
                                        # if they are in the same day/week.
     'cohorts': 5                       # How many cohorts (days/weeks/months)
                                        # to track.
     'grouping_key': 'user'},           # What key in an event should we tie
                                        # to a key in the action stream?

   'action':
     {'stream': CohortTest.FRONTPAGE_STREAM, # Stream users take actions on.
      'transform': lambda x: x               # Transformations on the stream.
      'unit': DateUnit.XX,                   # Track events in day/week/months.
      'repetitions': 14                   # How many days/weeks/months to track.
      'grouping_key': 'user_id'}          # What key in an event should we tie
                                          # to a key in the action stream?
  }

  Output:
  A metis-compatible query plan to return a cohort analysis.
  """
  cohort = plan['cohort']
  action = plan['action']
  kronos_url = plan.get('kronos_url', app.config['KRONOS_SERVER'])

  # Calculate the start and end dates, in Kronos time, of the
  # beginning and end of the cohort and action streams that will be
  # relevant.
  cohort_start = datetime_to_kronos_time(_date_to_datetime(cohort['start']))
  cohort_span = timedelta(**{cohort['unit']: cohort['cohorts']})
  cohort_end = cohort['start'] + cohort_span
  action_span = timedelta(**{action['unit']: action['repetitions']})
  action_end = cohort_end + action_span
  cohort_end = datetime_to_kronos_time(_date_to_datetime(cohort_end)) + 1
  action_end = datetime_to_kronos_time(_date_to_datetime(action_end)) + 1

  left = _cohort_stream_transform(kronos_url,
                                  cohort['stream'], cohort_start, cohort_end,
                                  cohort.get('transform'),
                                  cohort['grouping_key'], cohort['unit'])
  right = _cohort_stream_transform(kronos_url,
                                   action['stream'], cohort_start, action_end,
                                   action.get('transform'),
                                   action['grouping_key'], action['unit'])

  additional_action_time = (DateUnit.unit_to_kronos_time(action['unit']) *
                            action['repetitions'])

  joined = join(
    left,
    right,
    cond_and(cond(p('cohort.%s' % cohort['grouping_key']),
                  p('action.%s' % action['grouping_key']),
                  ConditionOpType.EQ),
             cond(p('action.%s' % TIMESTAMP_FIELD),
                  p('cohort.%s' % TIMESTAMP_FIELD),
                  ConditionOpType.GTE),
             cond(p('action.%s' % TIMESTAMP_FIELD),
                  f(FunctionType.ADD,
                    [p('cohort.%s' % TIMESTAMP_FIELD),
                     c(additional_action_time)]),
                  ConditionOpType.LT)),
    left_alias='cohort',
    right_alias='action')

  user_aggregated = agg(
    joined,
    {TIMESTAMP_FIELD: p('cohort.date'),
     'group': p('cohort.%s' % cohort['grouping_key']),
     'action_step': f(FunctionType.FLOOR,
                      [f(FunctionType.SUBTRACT,
                         [p('action.%s' % TIMESTAMP_FIELD),
                          p('cohort.%s' % TIMESTAMP_FIELD)]),
                       c(DateUnit.unit_to_kronos_time(action['unit']))])},
    []
    )

  aggregated = agg(
    user_aggregated,
    {TIMESTAMP_FIELD: p(TIMESTAMP_FIELD),
     'action_step': p('action_step')},
    [agg_op(AggregateType.COUNT, [], alias='cohort_actions')]
    )

  # TODO(marcua): Also sum up the cohort sizes, join with the plan.
  return aggregated


def cohort_response(plan, events):
  cohort = defaultdict(lambda: {'cohort_size': 0,
                                'action_dates': defaultdict(int)})
  for event in events:
    cohort_date = (kronos_time_to_datetime(event[TIMESTAMP_FIELD])
                   .date()
                   .isoformat())
    step = timedelta(seconds=
                     kronos_time_to_epoch_time(int(event['action_step'])))
    step = getattr(step, plan['action']['unit'])
    cohort[cohort_date]['action_dates'][step] = event['cohort_actions']
  return cohort
