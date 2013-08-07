from collections import defaultdict
from flask import jsonify
from flask import render_template
from flask import request
from time import mktime

from jia import app

from auth import require_auth
from lib.kronos.client import KronosClient
from lib.kronos.utils import is_kronos_reserved_key
from lib.kronos.utils import kronos_time_to_datetime

@app.route('/')
@require_auth
def index():
  return render_template("index.html")

@app.route('/get', methods=['POST'])
@require_auth
def get():
  # TODO(meelap): Jia can do some validation of stream name/times to avoid
  # network calls when they're unnecessary

  try:
    stream = request.form['stream']
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
    # Rickshaw format is a dict mapping stream names to a list of x,y
    # coordinates where the x-value is a timestamp and the y value is the value
    # of the stream name at that timestamp.
    rickshawdata = defaultdict(list)
    for point in kronosdata:
      time = kronos_time_to_datetime(point[KronosClient.TIMESTAMP_FIELD])
      time = int(mktime(time.timetuple()))
      for key,value in point.iteritems():
        if properties and key not in properties:
          continue
        if not is_kronos_reserved_key(key):
          rickshawdata[key].append({'x': time, 'y': value})
    return jsonify(rickshawdata)
  except Exception as e:
    return jsonify(error=repr(e))

@app.route('/streams', methods=['GET'])
@require_auth
def streams():
  try:
    kronosclient = KronosClient(app.config['KRONOS_URL'], blocking=False)
    streams = [stream for stream in kronosclient.get_streams()]
    return jsonify(streams=streams)
  except Exception as e:
    return jsonify(error=repr(e))
