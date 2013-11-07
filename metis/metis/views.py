from flask import jsonify
from flask import request

import metis

from metis import app
from metis.core import compute

@app.route('/1.0/index', methods=['GET'])
def index():
  return jsonify({
      'status': 'metisd',
      'version': metis.get_version()
      })

# TODO(usmanm): Add error handling everywhere.
@app.route('/1.0/get', methods=['POST'])
def get():
  # TODO(usmanm): `force` doesn't seem to work. Still need to send the correct
  # application/json header.
  request_json = request.get_json(force=True)
  stream = request_json['stream']
  start_time = request_json['start_time']
  end_time = request_json['end_time']
  transforms = request_json.get('transforms', [])
  response = {'status': 'ok'}
  response.update({
      'result': compute.execute_compute_task(stream, start_time, end_time,
                                             transforms, stream_out)
      })
  return jsonify(response)

# TODO(usmanm): Remove after testing.
@app.route('/test')
def test():
  import json, random, requests
  from metis.core import transform
  stream = 'locu.widget.view.sum.86400'
  start_time = 13692060000000000
  end_time = 13772154593060000
  response = requests.post('%s/1.0/events/get' % app.config['KRONOS_SERVER'],
                           data=json.dumps({'stream': stream,
                                            'start_time': start_time,
                                            'end_time': end_time}),
                           stream=True)
  events = []
  for event in response.iter_lines():
    if not event:
      continue
    event = json.loads(event)
    event['name'] = random.choice(['alice', 'bob', 'charlie', 'dick', 'eugene',
                                   'fraser', 'gomes'])
    events.append(event)
  spark_context = compute.CONTEXT_MANAGER.get_context()
  rdd = spark_context.parallelize(events)
  op = transform.FilterTransform('$total', 'gt', 140000)
  rdd = op.apply(spark_context, rdd)
  print '>>> FILTER', rdd.collect()[:10]
  op = transform.ProjectionTransform(['$total', 'opentable', 'name'])
  rdd = op.apply(spark_context, rdd)
  print '>>> PROJECTION', rdd.collect()[:10]
  op = transform.GroupByTimeTransform(864000000000*5)
  rdd = op.apply(spark_context, rdd)
  print '>>> TIME BUCKETS', rdd.collect()[:10]
  op = transform.GroupByTransform(['name'])
  rdd = op.apply(spark_context, rdd)
  print '>>> GROUPBY', rdd.collect()[:10]
  op = transform.ProjectionTransform(['$total', 'name'])
  rdd = op.apply(spark_context, rdd)
  print '>>> PROJECTION', rdd.collect()[:10]
  op = transform.AggregateTransform([{'op': 'count', 'alias': 'event_count'},
                                     {'op': 'sum', 'key': '$total'}])
  rdd = op.apply(spark_context, rdd)
  print '>>> AGGREGATE', rdd.collect()[:10]
  compute.CONTEXT_MANAGER.release_context(spark_context)
  return jsonify({})
