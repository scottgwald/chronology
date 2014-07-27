from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from dateutil.tz import tzutc
from datadiff.tools import assert_equal
from pykronos.utils.time import datetime_to_kronos_time
from random import randint
from random import random

from metis.conf import constants
from metis.convenience.cohort import cohort_queryplan
from metis.convenience.cohort import cohort_response
from metis.convenience.cohort import DateUnit
from tests.server import MetisServerTestCase


def datetime_to_date_str(dt):
  return dt.date().isoformat()


class CohortTestCase(MetisServerTestCase):
  EMAIL_STREAM = 'cohort_test_case.emails'
  FRONTPAGE_STREAM = 'cohort_test_case.frontpage'
  START_DATETIME = datetime(2011, 11, 18).replace(tzinfo=tzutc())
  ACTION_REPETITION_DAYS = 14
  EMAIL_WEEKS = [(0, 2), (2, 4), (1, 3), (3, 0), (4, 1)]
  NUM_USERS = 400
  MAX_DT = datetime(2020, 1, 1).replace(tzinfo=tzutc())

  def generate_data(self):
    user_ids = range(CohortTestCase.NUM_USERS)

    # Email stream: Users in groups 1-5 get an email in weeks 1 and 3,
    # 2 and 4, 3 and 5, 4 and 1, 5 and 2, respectively.
    # See `EMAIL_WEEKS`.
    user_dates = {}

    # Fill in expected_output, which is of
    # the form: {cohort_date: {cohort_size: NN,
    #                          action_dates: {action_date: num_actions}}}
    expected = defaultdict(lambda: {'cohort_size': 0,
                                    'action_dates': defaultdict(set)})

    for user_id in user_ids:
      weeks1, weeks2 = CohortTestCase.EMAIL_WEEKS[
        user_id % len(CohortTestCase.EMAIL_WEEKS)]
      week1 = CohortTestCase.START_DATETIME + timedelta(weeks=weeks1)
      date1 = week1 + timedelta(hours=randint(0, 72))
      week2 = CohortTestCase.START_DATETIME + timedelta(weeks=weeks2)
      date2 = week2 + timedelta(hours=randint(0, 72))
      week1_str = datetime_to_date_str(week1)
      week2_str = datetime_to_date_str(week2)
      user_dates[user_id] = ({'cohort': week1_str,
                              'cohort_date': week1,
                              'precise_date': date1},
                             {'cohort': week2_str,
                              'cohort_date': week2,
                              'precise_date': date2})
      expected[week1_str]['cohort_size'] += 1
      expected[week2_str]['cohort_size'] += 1
      self.kronos_client.put({
        CohortTestCase.EMAIL_STREAM: [
          {'user': user_id,
           constants.TIMESTAMP_FIELD: datetime_to_kronos_time(date1)},
          {'user': user_id,
           constants.TIMESTAMP_FIELD: datetime_to_kronos_time(date2)}
          ]
        })

    # Action stream: Users in group 1 hit the front page w/ 1/5
    # percent chance, group 2 with 2/5 chance, etc. Likelihood for
    # all users on day N is 1/N.
    for user_id in user_ids:
      day_to_min_action_dt = defaultdict(dict)
      for email_dates in user_dates[user_id]:
        for day in xrange(CohortTestCase.ACTION_REPETITION_DAYS):
          group_probability = (
            ((user_id % len(CohortTestCase.EMAIL_WEEKS)) + 1.0) /
            len(CohortTestCase.EMAIL_WEEKS))
          day_probability = 1.0 / (day + 1)
          action_probability = group_probability * day_probability
          if random() < action_probability:
            action_dt = email_dates['precise_date'] + timedelta(days=day)
            # This is the date that our cohort plan compares to.
            day_to_min_action_dt[action_dt.date()] = min(
              action_dt,
              day_to_min_action_dt.get(action_dt.date(),
                                       CohortTestCase.MAX_DT)
              )
            self.kronos_client.put({
              CohortTestCase.FRONTPAGE_STREAM: [
                {'user_id': user_id,
                 '@time': datetime_to_kronos_time(action_dt)}]
              })
            action_compare_dt = day_to_min_action_dt[action_dt.date()]
            for _email_dates in user_dates[user_id]:
              if (_email_dates['precise_date'] > action_compare_dt or
                  _email_dates['precise_date'] +
                  timedelta(CohortTestCase.ACTION_REPETITION_DAYS) <=
                  action_compare_dt):
                continue
              day = (action_compare_dt - _email_dates['precise_date']).days
              # Each user must be counted only once!
              expected[_email_dates['cohort']]['action_dates'][day].add(
                user_id)

    for cohort_name in expected:
      action_dates = expected[cohort_name]['action_dates']
      days = action_dates.keys()
      for day in days:
        action_dates[day] = len(action_dates[day])

    return expected

  def test_cohort(self):
    expected = self.generate_data()
    plan = {
      'cohort': {'stream': CohortTestCase.EMAIL_STREAM,
                 'start': CohortTestCase.START_DATETIME,
                 'cohorts': len(CohortTestCase.EMAIL_WEEKS),
                 'unit': DateUnit.WEEKS,
                 'grouping_key': 'user'},
      'action': {'stream': CohortTestCase.FRONTPAGE_STREAM,
                 'repetitions': CohortTestCase.ACTION_REPETITION_DAYS,
                 'unit': DateUnit.DAYS,
                 'grouping_key': 'user_id'}
      }
    metis_plan = cohort_queryplan(plan)
    events = self.query(metis_plan)
    cohort = cohort_response(plan, events)

    # Same set of cohorts.
    self.assertEqual(set(expected), set(cohort))
    # Action dates match for all cohorts.
    cohorts = expected.keys()
    for cohort_name in cohorts:
      self.assertEqual(set(expected[cohort_name]['action_dates']),
                       set(cohort[cohort_name]['action_dates']))
      assert_equal(dict(expected[cohort_name]['action_dates']),
                   dict(cohort[cohort_name]['action_dates']))
