# Metis

## Introduction

Metis is a HTTP compute service over streams of data stored in Kronos. It's
currently implemented as a thin wrapper around the
[Spark data processing engine](http://spark.apache.org/).

In the future we plan on making compute engines pluggable in Metis, and
supporting data sources other than Kronos. So similar to how Kronos lets you
store data in a storage backend of your choice, Metis will let you run compute
jobs on the framework of your choice.

## Settings details

Take a look at [settings.py.template](settings.py.template).  We tried
to document all of the settings pretty thoroughly.  If anything is
unclear, [file an issue](../../../issues?state=open) and we'll clarify!

## Get running in 5 minutes

First, check out Metis, add some default settings, and launch it
locally:

```bash
git clone https://github.com/Locu/chronology.git
cd chronology/metis
make installdeps
python runserver.py --port 8152 --config settings.py.template --debug
```

This walk-through assumes that you have PyKronos installed on your system and
a test Kronos instance is running at `http://localhost:8150`. See [here]
(../kronos/) on how to set up Kronos.

We'll be running some computations on the presidential campaigns contributions
dataset. Start by inserting the test data into your Kronos server. We will be
inserting around **100 thousand** events so make sure your test Kronos instance
can handle that. The events are fairly small and shouldn't take more than a few
megabytes of memory in case you want to use the `InMemoryBackend`.

```bash
cd chronology/kronos
python scripts/load_test_elections_data.py
```

A sample event dictionary of this test data looks like:

```python
{u'@id': u'9809c000-17ed-11e2-8000-0b89d16a9975',
 u'@time': 13504320000000000L,
 u'cand_id': u'P80003338',
 u'cand_nm': u'Obama, Barack',
 u'cmte_id': u'C00431445',
 u'contb_receipt_amt': u'6',
 u'contb_receipt_dt': u'17-OCT-12',
 u'contbr_city': u'PORTLAND',
 u'contbr_employer': u'NOT EMPLOYED',
 u'contbr_nm': u'BROOKS, MAGGIE',
 u'contbr_occupation': u'STUDENT',
 u'contbr_st': u'OR',
 u'contbr_zip': u'972171333',
 u'election_tp': u'G2012',
 u'file_num': u'897092',
 u'form_tp': u'SA17A',
 u'memo_cd': u'',
 u'memo_text': u'',
 u'null': [u''],
 u'receipt_desc': u'',
 u'tran_id': u'C26603850'}
```

Now let's use Metis to compute the total amount of money donated per day
during 2012, and how many donations each candidate got per day during that
interval. We also will request the result to be sorted is descending order
based on the amount of donations made each day.

```python
import json
import requests
from datetime import datetime
from datetime import timedelta
from metis.common.time import datetime_to_kronos_time
from metis.common.time import epoch_time_to_kronos_time
from metis.common.time import kronos_time_to_datetime
from metis.core.query.aggregate import Count
from metis.core.query.aggregate import GroupBy
from metis.core.query.aggregate import Sum
from metis.core.query.stream import KronosStream
from metis.core.query.transform import Aggregate
from metis.core.query.transform import Limit
from metis.core.query.transform import OrderBy
from metis.core.query.value import Constant
from metis.core.query.value import DateTrunc
from metis.core.query.value import Property
from pykronos import TIMESTAMP_FIELD

def query(plan):
  for line in requests.post('http://localhost:8152/1.0/query',
                            data=json.dumps({'plan': plan.to_dict()}),
                            stream=True).iter_lines():
    if not line:
      continue
    yield json.loads(line)

# We want to answer two questions:
# 1. Which day of 2012 got the highest donation amount?
# 2. On that day (from 1), which candidates got donations and how many
#    donations?

# First let's find the day that got the most donations.

# All donation events which happened in the year 2012.
stream = KronosStream('http://localhost:8150',
                      'donations',
                      datetime_to_kronos_time(datetime(2012, 1, 1)),
                      datetime_to_kronos_time(datetime(2012, 12, 31)))

# We need to aggregate by summing up values for the `contb_receipt_amt`
# property.
aggregates = [Sum([Property('contb_receipt_amt')], alias='total_donations')]

# We need to group by TIMESTAMP_FIELD rounded down to the start of each day.
group_by = GroupBy(DateTrunc([Property(TIMESTAMP_FIELD),
                              Constant(DateTrunc.Unit.DAY)],
                             alias=TIMESTAMP_FIELD))

aggregate = Aggregate(stream, group_by, aggregates)

# Order by the new `total` field created in descending order and pick the
# first event.
plan = Limit(OrderBy(aggregate, [Property('total_donations')], reverse=True),
             1)

events = list(query(plan))
assert len(events) == 1

# Get the event for the day during which the maximum donations were made.
event = events[0]
day = kronos_time_to_datetime(event[TIMESTAMP_FIELD]).date()

print 'A total of $%f were donated on %s.' % (event['total_donations'], day)
# > A total of $291489.300000 were donated on 2012-10-17.

# Now let's find the number of donations received per candidate during the day
# from above.

stream = KronosStream('http://localhost:8150',
                      'donations',
                      datetime_to_kronos_time(day),
                      datetime_to_kronos_time(day + timedelta(days=1)))

# We need to aggregate by simply counting the number of events in each
# group.
aggregates = [Count([], alias='num_donations')]

# We need to group by the `cand_nm` property.
group_by = GroupBy(Property('cand_nm', alias='candidate_name'))

plan = Aggregate(stream, group_by, aggregates)

events = list(query(plan))
assert len(events) == 4

for event in sorted(events, key=lambda e: e['num_donations']):
  print '  %s got %d donations' % (event['candidate_name'],
                                   event['num_donations'])
# > On that day the following people got donations:
# >   Johnson, Gary Earl got 1 donations
# >   Stein, Jill got 2 donations
# >   Romney, Mitt got 310 donations
# >   Obama, Barack got 3404 donations
```

## Deployment

Metis is a [Flask](http://flask.pocoo.org/) application and can be deployed
like any standard Flask application. You can find the different deployment
options [here](http://flask.pocoo.org/docs/deploying/).
