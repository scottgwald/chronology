from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from dateutil.tz import tzutc

from metis import app
from metis.common.time import datetime_to_kronos_time
from metis.common.time import kronos_time_to_datetime
from metis.common.time import kronos_time_to_epoch_time
from metis.common.time import epoch_time_to_kronos_time
from metis.conf.constants import TIMESTAMP_FIELD
from metis.core.query.aggregate import Count
from metis.core.query.aggregate import GroupBy
from metis.core.query.aggregate import Min
from metis.core.query.condition import Condition
from metis.core.query.stream import KronosStream
from metis.core.query.transform import Aggregate
from metis.core.query.transform import Join
from metis.core.query.transform import Project
from metis.core.query.value import Add
from metis.core.query.value import Constant
from metis.core.query.value import Floor
from metis.core.query.value import Property
from metis.core.query.value import Subtract


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
  start_stream = KronosStream(kronos_url, stream, start, end)
  if transform:
    transformed = transform(start_stream)
  else:
    transformed = start_stream
  projected = Project(transformed,
                      [Property(TIMESTAMP_FIELD,
                                alias=TIMESTAMP_FIELD),
                       Property(grouping_key,
                                alias=grouping_key),
                       Floor([Property(TIMESTAMP_FIELD),
                              Constant(DateUnit.unit_to_kronos_time(unit)),
                              Constant(start)],
                             alias='date')])
  # This leaves us with a single event per (user, unit time) pair.
  aggregated = Aggregate(
    projected,
    GroupBy([Property(grouping_key,
                      alias=grouping_key),
             Property('date',
                      alias='date')]),
    # The first time the user performed the event in that bucket.
    [Min([Property(TIMESTAMP_FIELD)], alias=TIMESTAMP_FIELD)])
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

  left.alias = 'cohort'
  right.alias = 'action'
  
  joined = Join(left,
                right,
                (Condition(Condition.Op.EQ,
                           Property('cohort.%s' % cohort['grouping_key']),
                           Property('action.%s' % action['grouping_key'])) &
                 Condition(Condition.Op.GTE,
                           Property('action.%s' % TIMESTAMP_FIELD),
                           Property('cohort.%s' % TIMESTAMP_FIELD)) &
                 Condition(Condition.Op.LT,
                           Property('action.%s' % TIMESTAMP_FIELD),
                           Add([Property('cohort.%s' % TIMESTAMP_FIELD),
                                Constant(additional_action_time)]))))

  user_aggregated = Aggregate(
    joined,
    GroupBy([Property('cohort.date', alias=TIMESTAMP_FIELD),
             Property('cohort.%s' % cohort['grouping_key'], alias='group'),
             Floor([Subtract([Property('action.%s' % TIMESTAMP_FIELD),
                              Property('cohort.%s' % TIMESTAMP_FIELD)]),
                    Constant(DateUnit.unit_to_kronos_time(action['unit']))],
                   alias='action_step')]),
    [Count([], alias='count')]
    )

  aggregated = Aggregate(
    user_aggregated,
    GroupBy([Property(TIMESTAMP_FIELD, alias=TIMESTAMP_FIELD),
             Property('action_step', alias='action_step')]),
    [Count([], alias='cohort_actions')])

  # TODO(marcua): Also sum up the cohort sizes, join with the plan.
  return aggregated.to_dict()


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
