from collections import defaultdict
import os
from time import mktime

from flask import jsonify
from flask import request
from flask import send_from_directory

from jia import app

from auth import require_auth
from pykronos.client import KronosClient
from pykronos.client import TIMESTAMP_FIELD
from pykronos.utils.time import is_kronos_reserved_key
from pykronos.utils.time import kronos_time_to_datetime

@app.route('/')
@require_auth
def index():
  return send_from_directory(os.path.join(app.config['APPROOT'], 'jia/static/html'), 'index.html')

@app.route('/get', methods=['POST'])
@require_auth
def get():
  # TODO(meelap): Jia can do some validation of stream name/times to avoid
  # network calls when they're unnecessary

  try:
    stream = request.form['stream_name']
    properties = frozenset(request.form.getlist('properties[]'))
    start = request.form['start_time']
    end = request.form['end_time']

    # Fetch data from Kronos
    # TODO(meelap): Optimization - pass desired properties to Kronos
    kronosclient = KronosClient(app.config['KRONOS_URL'], blocking=False)
    kronosdata = kronosclient.get(stream, start, end)
    
    # Convert events returned by Kronos to a format suitable for rickshaw
    # Kronos format is a list of dicts where each dict contains '@time' and
    # various other key/value pairs.
    # Output format is a dict mapping stream names to a list of x,y
    # coordinates where the x-value is a timestamp and the y value is the value
    # of the stream name at that timestamp.
    timeseries = defaultdict(list)
    for point in kronosdata:
      time = kronos_time_to_datetime(point[TIMESTAMP_FIELD])
      time = int(mktime(time.timetuple()))
      for key,value in point.iteritems():
        if properties and key not in properties:
          continue
        if not is_kronos_reserved_key(key):
          timeseries[key].append({'x': time, 'y': value})
    return jsonify(timeseries)
  except Exception as e:
    return jsonify(error=repr(e))

@app.route('/streams', methods=['GET'])
@require_auth
def streams():
  try:
    kronosclient = KronosClient(app.config['KRONOS_URL'], blocking=False)
    streams = {stream[0]:stream[1] for stream in kronosclient.get_streams()}
    return jsonify(streams=streams)
  except Exception as e:
    return jsonify(error=repr(e))
