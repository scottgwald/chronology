import cjson as json
import requests
import time

from threading import Thread, Lock
from collections import defaultdict

class KronosClientException(Exception):
  pass


class KronosClient(object):
  """
  Initialize a Kronos client that can connect to a server at `http_url`

  Put requests are non-blocking if `blocking`=False.
  If non-blocking, `sleep_block` specifies the frequency of
    a background thread that flushes events to the server.
  """
  def __init__(self, http_url, blocking=True, sleep_block=0.1):
    self._put_url = '%s/1.0/events/put' % http_url
    self._get_url = '%s/1.0/events/get' % http_url
    self._index_url = '%s/1.0/index' % http_url
    self._blocking = blocking
    if not blocking:
      self._sleep_block = sleep_block
      self._setup_nonblocking()
    self._setup_remote_keys()

  def index(self):
    return requests.get(self._index_url).json()

  def put(self, event_dict):
    """
    Sends a dictionary of `event_dict` of the form {stream_name:
    [event, ...], ...}  to the server.
    
    The `blocking` parameter allows the request to block until the
    server responds, and returns some information on the response.
    Here's an example:

    {u'stream_name_1': 3, u'stream_name_2': 1, u'@took': u'1ms'}
      -> put 3 events on stream_name_1
      -> put 1 event on stream_name_2
      -> put took 1ms to complete

    If `blocking` is false and the process running the client ends
    before flushing the pending data to the server, you might lose
    that data.  Calling `flush` will block until all pending data has
    been acknowledged by the server.
    """
    if self._blocking:
      return self._put(event_dict)
    else:
      self._put_lock.acquire()
      self._put_queue.append(event_dict)
      self._put_lock.release()

  def get(self, stream, start_time, end_time, start_id=None):
    """
    Queries a stream with name `stream` for all events between
    `start_time` and `end_time`.  An optional `start_id` allows the
    client to restart from a failure, specifying the last ID they
    read.
    """
    stream_params = {
      'stream': stream,
      'end_time': end_time
    }
    if start_id:
      stream_params['start_id'] = start_id
    else:
      stream_params['start_time'] = start_time

    errors = []
    last_id = None
    done = False
    while not done:
      try:
        response = requests.post(self._get_url,
                                 data=json.encode(stream_params),
                                 stream=True)
        if response.status_code != requests.codes.ok:
          raise KronosClientException('Bad server response code %d' %
                                      response.status_code)
        for line in response.iter_lines():
          if line:
            event = json.decode(line)
            last_id = event[self.id_key]
            yield event
        done = True
      except Exception, e:
        errors.append(e)
        if len(errors) == 10:
          raise KronosClientException(errors)
        if last_id != None:
          if 'start_time' in stream_params:
            del stream_params['start_time']
          stream_params['start_id'] = last_id
        time.sleep(len(errors) * 0.1)
          
  def _setup_nonblocking(self):
    self._put_queue = []
    self._put_lock = Lock()

    me = self
    class PutThread(Thread):
      def __init__(self):
        Thread.__init__(self)
        self.daemon = True
      def run(self):
        while True:
          me.flush()
          time.sleep(me._sleep_block)
    PutThread().start()

  def flush(self):
    if self._blocking:
      return
    self._put_lock.acquire()
    old_queue = None
    if self._put_queue:
      old_queue = self._put_queue
      self._put_queue = []
    self._put_lock.release()
    if old_queue:
      stream_name_to_events_map = defaultdict(list)
      for event_dict in old_queue:
        for stream_name, events in event_dict.iteritems():
          stream_name_to_events_map[stream_name].extend(events)
      self._put(stream_name_to_events_map)

  def _setup_remote_keys(self):
    index = self.index()
    self.id_key = index['fields']['id']
    self.time_key = index['fields']['timestamp']

  def _put(self, event_dict):
    response = requests.post(self._put_url, data=json.encode(event_dict))
    if response.status_code != requests.codes.ok:
      raise KronosClientException('Received response code %s with errors %s' %
                                  (response.status_code,
                                   response.json().get('@errors', '')))
    response_dict = response.json()
    errors = response_dict.get('@errors')
    if errors:
      raise KronosClientException('Encountered errors %s' % errors)
    return response_dict

