from flask import json
from flask import jsonify
from flask import request
from flask import Response

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
@app.route('/1.0/events/get', methods=['POST'])
def get():
  # TODO(usmanm): `force` doesn't seem to work. Still need to send the correct
  # application/json header.
  request_json = request.get_json(force=True)
  stream = request_json['stream']
  start_time = request_json['start_time']
  end_time = request_json['end_time']
  transforms = request_json.get('transforms', [])
  namespace = request_json.get('namespace',
                               app.config['DEFAULT_READ_NAMESPACE'])
  return Response(('%s\r\n' % json.dumps(event)
                   for event in compute.execute_compute_task(stream,
                                                             namespace,
                                                             start_time,
                                                             end_time,
                                                             transforms)),
                  mimetype='application/json')
