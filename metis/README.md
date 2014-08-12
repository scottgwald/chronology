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
cd chronology/kronos
make installdeps
python runserver.py --port 8155 --config settings.py.template --debug
```

This walk-through assumes that you have PyKronos installed on your system and
a test Kronos instance is running at `http://localhost:8150`. See [here]
(../kronos/) on how to set up Kronos.

We'll be running some computations on the presidential campaigns contributions
dataset. Start by downloading the dataset:

```bash
wget ftp://ftp.fec.gov/FEC/Presidential_Map/2012/P00000001/P00000001-CA.zip
unzip P00000001-CA.zip
rm P00000001-CA.zip
mv P00000001-CA.csv donations.csv
```

Now let's transform this tabular CSV data into events and store them in Kronos.
We will be inserting around **100 thousand** events so make sure your test 
Kronos instance can handle that. The events are fairly small and shouldn't take more than a few megabytes of memory in case you want to use the 
`InMemoryBackend`.

```python
import csv
from dateutil.parser import parse
from pykronos import KronosClient
from pykronos import TIMESTAMP_FIELD

events = []
with open('donations.csv') as f:
  rows = csv.DictReader(f)
  for row in rows:
    row[TIMESTAMP_FIELD] = parse(row['contb_receipt_dt'])
    events.append(row)

kc = KronosClient('http://localhost:8150')
kc.put({'donations': events})
```

A sample event dictionary:

```python
{None: [''],
 '@time': datetime.datetime(2012, 8, 27, 0, 0),
 'cand_id': 'P80003353',
 'cand_nm': 'Romney, Mitt',
 'cmte_id': 'C00431171',
 'contb_receipt_amt': '1000',
 'contb_receipt_dt': '27-AUG-12',
 'contbr_city': 'TIGARD',
 'contbr_employer': 'INTEL CORPORATION',
 'contbr_nm': 'SMITH, VISTON R. JR.',
 'contbr_occupation': 'OPERATIONS MANAGER',
 'contbr_st': 'OR',
 'contbr_zip': '972242918',
 'election_tp': 'G2012',
 'file_num': '896743',
 'form_tp': 'SA17A',
 'memo_cd': '',
 'memo_text': '',
 'receipt_desc': '',
 'tran_id': 'SA17.2209403'}
```

Now let's use Metis to compute the total amount of money donated per day
during 2012, and how many donations each candidate got per day during that
interval. We also will request the result to be sorted is descending order
based on the amount of donations made each day.

```python
import json
import requests
from datetime import datetime
from metis.common.time import datetime_to_kronos_time
from metis.common.time import epoch_time_to_kronos_time
from metis.common.time import kronos_time_to_datetime
from metis.core.query.primitives import (kstream,
                                         p,
                                         c,
                                         f,
                                         agg_op,
                                         agg,
                                         order_by,
                                         AggregateType,
                                         FunctionType)
from pykronos import TIMESTAMP_FIELD

def query(plan):
  for line in requests.post('http://localhost:8155/1.0/query',
                            data=json.dumps({'plan': plan}),
                            stream=True).iter_lines():
    if not line:
      continue
    yield json.loads(line)

# All donation events which happened in the year 2012.
events_stream = kstream('donations',
                       datetime_to_kronos_time(datetime(2012, 1, 1)),
                       datetime_to_kronos_time(datetime(2012, 12, 31)))

# We want two aggregates:
# 1. Sum of all the `contb_receipt_amt` fields of events in one day.
# 2. Number of donation per candidate in one day.
aggregates = [agg_op(AggregateType.SUM,
                     [p('contb_receipt_amt')],
                     'total'),
              agg_op(AggregateType.VALUE_COUNT,
                     [p('cand_nm')],
                     'candidates')]

# We need to group by TIMESTAMP_FIELD rounded down to the start of each day.
group_bys = {
  TIMESTAMP_FIELD: f(FunctionType.FLOOR,
                     [p(TIMESTAMP_FIELD),
                     c(epoch_time_to_kronos_time(60*60*24))])
  }

aggregate = agg(events_stream, group_bys, aggregates)

# Order by the new `total` field created as a result of the `agg` operation.
events = list(query(order_by(aggregate, [p('total')], reverse=True)))

# Get the event for the day during which the maximum donations were made.
event = events[0]
day = kronos_time_to_datetime(event[TIMESTAMP_FIELD]).date()

print 'A total of $%f were donated on %s.' % (event['total'], day)
print 'On that day the following people got donations:'
for candidate, num in event['candidates'].iteritems():
print '  %s got %d donations' % (candidate, num)
# > A total of $291489.300000 were donated on 2012-10-17.
# > On that day the following people got donations:
# >   Romney, Mitt got 234 donations
# >   Obama, Barack got 2746 donations
```
