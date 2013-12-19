from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil.tz import tzutc
from pykronos.utils.time import datetime_to_kronos_time
from pykronos.utils.time import EPOCH
from metis.core.query.enums import ConditionOpType
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
from metis.core.query.primitives import TIME


class DateUnit(object):

  DAYS = 'days'
  WEEKS = 'weeks'
  MONTHS = 'months'

  @staticmethod
  def round_date_down(the_date, unit):
    if unit == DateUnit.WEEKS:
      return the_date - timedelta(days=the_date.weekday())
    elif unit == DateUnit.MONTHS:
      return date(the_date.year, the_date.month, 1)
    return the_date


def _date_to_datetime(d):
  return datetime(d.year, d.month, d.day).replace(tzinfo=tzutc())


def _cohort_stream_transform(kronos_url, stream, start, end,
                             transform, grouping_key, unit,
                             event_alias):
  date_alias = '%s_date' % event_alias
  size_alias = '%s_size' % event_alias
  start_stream = kstream(stream, start, end, kronos_url)
  transformed = transform(start_stream)
  projected = proj(transformed,
                    [p(TIME), p(grouping_key),
                     f('round_date_down', [p(TIME), c(unit)],
                       alias=date_alias)])
  aggregated = agg(projected,
                   [grouping_key, date_alias],
                   [agg_op('min', [p(TIME)], alias=TIME),
                    agg_op('count', [], alias=size_alias)])
  return aggregated


def cohort_queryplan(params):
  """
  Input:
  {
   'kronos_url': 'http://...',
   'cohort':
    {'stream': CohortTest.EMAIL_STREAM,        # Stream to define cohort from.
     'transform': lambda x: x,                 # Transformations on the stream.
     'start': date.now(),                      # The day of the first cohort.
     'unit': DateUnit.XX,                      # Users are in the same cohort
                                               # if they are in the same day/week.
     'cohorts': 5                              # How many cohorts (days/weeks/months)
                                               # to track.
     'grouping_key': 'user'},                  # What key in an event should we tie
                                               # to a key in the action stream?

   'action':
     {'stream': CohortTest.FRONTPAGE_STREAM,   # Stream users take actions on.
      'transform': lambda x: x                 # Transformations on the stream.
      'unit': DateUnit.XX,                     # Track events in day/week/months.
      'repetitions': 14                        # How many days/weeks/months to track.
      'grouping_key': 'user_id'}               # What key in an event should we tie
                                               # to a key in the action stream?
  }

  Output:
  A metis-compatible query plan to return a cohort analysis.
  """
  cohort = params['cohort']
  action = params['action']
  kronos_url = params['kronos_url']

  # Calculate the start and end dates, in Kronos time, of the
  # beginning and end of the cohort and action streams that will be
  # relevant.
  cohort['start'] = DateUnit.round_date_down(cohort['start'], cohort['unit'])
  cohort_start = datetime_to_kronos_time(_date_to_datetime(cohort['start']))
  cohort_end = (cohort['start'] +
                timedelta(**{cohort['unit']: cohort['cohorts']}))
  action_span = timedelta(**{action['unit']: action['repetitions']})
  action_end = cohort_end + action_span                
  cohort_end = datetime_to_kronos_time(_date_to_datetime(cohort_end))
  action_end = datetime_to_kronos_time(_date_to_datetime(action_end))

  left = _cohort_stream_transform(kronos_url,
                                  cohort['stream'], cohort_start, cohort_end,
                                  cohort['transform'],
                                  cohort['grouping_key'], cohort['unit'],
                                  'cohort')
  right = _cohort_stream_transform(kronos_url,
                                   action['stream'], cohort_start, action_end,
                                   action['transform'],
                                   action['grouping_key'], action['unit'],
                                   'action')

  additional_action_time = datetime_to_kronos_time(EPOCH + action_span)
  join_stream = join(left,
                     right,
                     cond_and([
                       cond(p(cohort['grouping_key']),
                            p(action['grouping_key']),
                            ConditionOpType.EQ),
                       cond(p(TIME), p(TIME), ConditionOpType.LT),
                       cond(f('add', [p('cohort_date'), c(additional_action_time)]),
                            p(TIME),
                            ConditionOpType.GT)]),
                     'cohort_date')

  aggregated = agg(join_stream,
                   [p(TIME), p('action_date')],
                   [agg_op('count', [], alias='cohort_actions')])
  
  # TODO(marcua): Also sum up the cohort sizes, join with the plan.
  # TODO(marcua): remove print after debugging is done
  import json; print json.dumps(aggregated, indent=2, sort_keys=True)
  return aggregated
  
