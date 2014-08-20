import json
 
from flask import jsonify
from flask import request
from flask import Response

import metis

from metis import app
from metis.core.execute.service import service

@app.route('/1.0/index', methods=['GET'])
def index():
  return jsonify({
      'status': 'metisd',
      'version': metis.get_version()
      })

# TODO(usmanm): Add error handling everywhere.
@app.route('/1.0/query', methods=['POST'])
def query():
  # TODO(usmanm): `force` doesn't seem to work. Still need to send the correct
  # application/json header.
  request_json = request.get_json(force=True)
  plan = request_json['plan']
  return Response(('%s\r\n' % json.dumps(event)
                   for event in service.execute_plan(plan)),
                  mimetype='application/json')
