from collections import defaultdict
from flask import jsonify
from flask import render_template
from flask import request
from time import mktime

from jia import app
from lib.kronos.client import KronosClient
from lib.kronos.utils import is_kronos_reserved_key
from lib.kronos.utils import kronos_time_to_datetime

@app.route('/')
def index():
  return render_template("index.html")

@app.route('/test')
def test():
  return render_template("test.html")

@app.route('/get', methods=['POST'])
def get():
  # TODO(meelap): Jia can do some validation of stream name/times to avoid
  # network calls when they're unnecessary

  try:
    stream = request.form['stream']
    start = request.form['start_time']
    end = request.form['end_time']
    kronosclient = KronosClient(app.config['KRONOS_URL'], blocking=False)
    kronosdata = kronosclient.get(stream, start, end)
    rickshawdata = defaultdict(list)
    for point in kronosdata:
      time = kronos_time_to_datetime(point[KronosClient.TIMESTAMP_FIELD])
      time = int(mktime(time.timetuple()))
      for key,value in point.iteritems():
        if not is_kronos_reserved_key(key):
          rickshawdata[key].append({'x': time, 'y': value})
    return jsonify(rickshawdata)
  except Exception as e:
    return jsonify(error=repr(e))

@app.route('/streams', methods=['GET'])
def streams():
  try:
    kronosclient = KronosClient(app.config['KRONOS_URL'], blocking=False)
    streams = [stream for stream in kronosclient.get_streams()]
    return jsonify(streams=streams)
  except Exception as e:
    return jsonify(error=repr(e))
