# Kronos

## Introduction

Kronos is a time series storage engine.  It can store streams of data
(e.g., user click streams, machine cpu utilization) and retrieve all
events you've stored over a given time interval.  Kronos exposes an
HTTP API to send, retrieve, and delete events.  We've written Kronos
backends for memory (testing),
[Cassandra](http://www.datastax.com/dev/blog/advanced-time-series-with-cassandra)
(low latency), and [S3](http://aws.amazon.com/s3/) (high throughput),
with more to come.

GoDaddy's Locu team currently uses it to store all user and machine
data that power our reports and analyses.

## Get running in 5 minutes

First, check out Kronos, add some default settings, and launch it
locally:

```bash
git clone https://github.com/Locu/kronos.git
cd kronos/kronos
make installdeps
python runserver.py --port 8151 --config settings.py.template --debug
```

Then, from the `kronos/pykronos` directory, run a `python` shell, and
insert/retrieve some data:

```python
from datetime import datetime
from datetime import timedelta
from dateutil.tz import tzutc
from pykronos.client import KronosClient
kc = KronosClient('http://localhost:8151', namespace='demo')
kc.put({'yourproduct.website.clicks': [
  {'user': 35, 'num_clicks': 10}]})
for event in kc.get('yourproduct.website.clicks',
                    datetime.now(tz=tzutc()) - timedelta(minutes=5),
                    datetime.now(tz=tzutc())):
  print event
```

On the first line, we created a Kronos client to speak with the server
we just started.  On the second line, we've put a single event on the
`yourproduct.website.clicks` clickstream.  Finally, we retrieve all
`yourproduct.website.clicks` events that happened in the last five
minutes.

If you wish to see a more detailed example of the Kronos API, check
out the more detailed [pykronos example](../pykronos/).

## Settings details

Take a look at [settings.py.template](settings.py.template).  We tried
to document all of the settings pretty thoroughly.  If anything is
unclear, [file an issue](../../../issues?state=open) and we'll clarify!

## Backends

### Memory 

The in-memory backend is mostly used for testing.  Here's a
sample `storage` configuration for it:

```python
storage = {
  'memory': {
    'backend': 'memory.InMemoryStorage',
    'max_items': 50000
  },
}
```

There's only one parameter, `max_items`, which specifies the
largest number of events you can store before it starts acting funny.

### Cassandra

Our Cassandra backend is targeted at low-latency event storage, but
internally we use it even for relatively bursty high-throughput
streams.  Here's a sample `storage` configuration:

```python
storage = {
  'cassandra': {
    'backend': 'cassandra.CassandraStorage',
    'hosts': ['127.0.0.1'],
    'keyspace_prefix': 'kronos_test',
    'replication_factor': 3,
    'default_timewidth_seconds': 60 * 60, # 1 hour.
    'default_shards_per_bucket': 3,
    'read_size': 1000
  }
}
```

Our design for the Cassandra backend is heavily influenced by [a great
blog post with
illustrations](http://www.datastax.com/dev/blog/advanced-time-series-with-cassandra)
on the topic.  Here are what the parameters above mean:

  * `hosts` is a list of Cassandra nodes to connect to.

  * `keyspace_prefix` is a prefix that identifies the keys in
    Cassandra. We typically use it to run `kronos_dev`,
    `kronos_staging`, and `kronos_production` keyspaces on the same
    Cassandra cluster.

  * `replication_factor` is the number of Cassandra nodes to replicate
    each data item to.  Note that this value is set at the
    initialization of a keyspace, so if you change it, existing
    keyspaces will be at their previous replication factor.  You'll
    have to manually change the replication factor of existing
    keyspaces.

  * `default_timewidth_seconds` is the number of seconds worth of data
    to bucket together.  The Cassandra backend stores all events that
    happened within the same time span (e.g., one hour) together.  Set
    this to a small timewidth to reduce latency (you won't have to
    look through a bunch of irrelevant events to find a small
    timespan), and to a high timewidth to increase throughput (you
    won't have to look through a bunch of buckets to aggregate all of
    the data of a large timespan).  We tend to go for an hour as a default.

  * `default_shards_per_bucket` makes up for the white lie we told in
    the last bullet point.  Rather than storing all events that happen
    during a given timespan in a single bucket (which will put
    pressure on a single Cassandra row key/nodes storing that key), we
    can shard each bucket across `default_shards_per_bucket` shards
    for each timewidth.  As an example, setting
    `default_shards_per_bucket` will store each hour worth of data in
    one of three shards, decreasing the pressure on any one Cassandra
    row key.

  * `read_size` is probably not something you will play around with
    too much.  If a single time width's shard contains a lot of
    events, the Cassandra client driver will transparently iterate
    through them in chunks of this size.

### ElasticSearch

** Under construction---don't use it yet **

### S3

** Under construction---don't use it yet **

## Deployment

We deploy Kronos with
[uWSGI](http://uwsgi-docs.readthedocs.org/en/latest/).  To see our
starter scripts for doing this, check out
[install_kronosd.py](scripts/install_kronosd.py),
[uwsgi.ini](scripts/uwsgi.ini), and
[kronosd.init.d](scripts/kronosd.init.d).  If anything is unclear,
[file an issue](../../../issues?state=open) and we'll clarify!

## Design goals, limitations, and comparisons to other systems

### Put + Get + Delete

There's a class of useful event logging/streaming systems allow you to
programmatically put events onto a stream via another log collector
(e.g., apache logs, syslogd), or an API, filter/transform the data in
some way, and send it off to another backend (e.g., S3, ElasticSearch,
etc.).  If you wish to retrieve the data you just logged, you must
interact with the system you sent it off to (e.g., query ElasticSearch
directly).  [Logstache](http://logstash.net/) and
[Fluentd](http://www.fluentd.org/) are good examples of such systems.

In the Kronos API, we've included the ability to retrieve and delete
your data from the data source after it's been sent to a backend.
This decision has its pros and cons.

The key benefit of supporting `get` and `delete` endpoints is that
Kronos is closer to a one-stop shop for your time series data.  After
inserting an event to a Kronos datastore, you can use the same Kronos
client API to retrieve events.  Because Kronos allows developers to
retrieve events based on the time they occur, the developer need not
understand the underlying storage format, providing them with
[physical data
independence](https://en.wikipedia.org/wiki/Data_independence).  With
the delete endpoint, a developer that accidentally writes faulty log
data to a stream can quickly delete that data before it muddies up
downstream processes.

If Kronos didn't support `get` and `delete`, it wouldn't be the end of
the world, but it would put more onus on the developer to understand
multiple systems. Consider an S3 backend that can only store events as
they occur, but not retrieve them based on when they occur.  A
developer wishing to crunch data that happened in a particular week
now has to programmatically list all files in an S3 bucket, identify
file names with the appropriate dates for that week, and then filter
for events in those files outside of the time frame they wish to
analyze.

Allowing developers to retrieve events from storage backends through
Kronos has its drawbacks.  First, data must flow through an extra hop:
Kronos retrieves data from a storage backend before sending it off to
the user.  Second, building `get` and `delete` endpoints for storage
backends requires more work on behalf of people writing new Kronos
backends.

Finally, and perhaps most controvertially, providing a `delete`
endpoint on an append-mostly system, while useful, has performance
implications on some backends.  While implementing event deletion in
the Kronos Cassandra backend was straightforward, deleting a single
event from a several hundred megabyte log file in S3 required making
difficult consistency and latency tradeoffs.  We imagine that many
`delete` implementations, if implemented, will not be as performant or
consistent as their `put` and `get` cousins.

### Time-based 

Every event happens at a particular time.  You can insert events that
happen at particular times (regardless of the current wall clock time)
and retrieve all events that happened between two times.  Developers
do not have to think about how these events will be stored or
retrieved, or the order in which events arrive.

Making time a first class citizen in a time series storage engine was a
natural fit.  Developers using Kronos appreciate that they can quickly
retrieve last month's data to do a quick analysis, and the learning
time to store and retrieve events is low.  Since developers can store
events that happened last month with the same ease that they can store
events that just happened, there's not a lot of mental overhead in
doing things like migrating or restoring data.

Providing a time-based API has its challenges.  In developing a Kronos
backend, one has to consider events that arrive out of order, and in
returning events to users, backends have to ensure time ordering on
events.  Kyle Kingsbury has a great [writeup on the challenges of
using
timestamps](http://aphyr.com/posts/299-the-trouble-with-timestamps) in
distributed systems design, and Kronos is no exception to this great
analysis.

In particular, be careful in using Kronos for publish-subscribe
messaging applications that a system like
[Kafka](https://kafka.apache.org/) might be better suited for.  Just
because Kronos allows you to query for events that happened in the
past minute doesn't mean all events that happened in the last minute
have made their way across the network and into a storage system.
While we use Kronos for operational monitoring applications that
require us to look at recent data, we only perform analyses that
inform business decisions after giving events enough time (e.g., 5-20
minutes, or even an hour) to make their way to their final
destination.

### Multiple backends

Unlike systems such as [InfluxDB](http://influxdb.com/) or
[Kafka](https://kafka.apache.org/), Kronos does not have a single
backend that it relies on.  This allows us to provide different
backends for different use cases.  For example, while the Cassandra
backend provides relatively low latency access, the S3 backend
optimizes for write and read throughput.  The breadth of storage
options places more effort on Kronos users to decide which backends to
store and retrieve data from for different use-cases.

### Avoid more complex operations

We use Kronos in conjunction with tools like
[Spark](https://spark.apache.org/) and
[PANDAS](http://pandas.pydata.org/pandas-docs/stable/index.html) to do
data analysis on several streams, and leave complex operations (e.g.,
filters, aggregates, joins) to those systems.  The
[Metis](../metis/) project facilitates time series analyses
of this form on top of Kronos, and a natural question would be whether
Kronos could perform some amount of projection, filtering, or
aggregation on behalf of a user to speed up these queries.

At this point, the Kronos `get` endpoint does not support anything
other than time-based event retrieval to keep our design simple.  In
the future, we imagine pushing down operations such as filters or
projections into the Kronos API, but want to tread lightly until we
have a good design for this.
