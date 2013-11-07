import threading

from pyspark import SparkContext

from metis import app
from metis.core import transform
from pykronos.client import KronosClient
from metis.utils.decorators import async

KRONOS = KronosClient(app.config['KRONOS_SERVER'])

class SparkContextManager(object):
  def __init__(self):
    if app.debug:
      self._max_contexts = 1
    else:
      self._max_contexts = app.config['NUM_WORKERS']
    self._cv = threading.Condition()
    self._contexts_created = 0
    self._queue = []

  def release_context(self, context):
    self._cv.acquire()
    self._queue.append(context)
    self._cv.notify()
    self._cv.release()

  def _create_context(self):
    # Also ship the metis zip file so worker nodes can deserialize metis
    # internal objects.
    context = SparkContext(app.config['SPARK_MASTER'],
                           'Metis-%s' % self._contexts_created,
                           pyFiles=[app.config['METIS_ZIP_FILE']])
    return context

  def get_context(self):
    self._cv.acquire()
    while True:
      if self._queue:
        context = self._queue.pop()
        self._cv.release()
        return context
      if self._contexts_created < self._max_contexts:
        context = self._create_context()
        self._contexts_created += 1
        self._cv.release()
        return context
      self._cv.wait()

CONTEXT_MANAGER = SparkContextManager()


def _get_kronos_rdd(spark_context, stream, start_time, end_time):
  events = KRONOS.get(stream, start_time, end_time)
  return spark_context.parallelize(events)


def execute_compute_task(stream_in, start_time, end_time, transforms,
                         stream_out):
  spark_context = CONTEXT_MANAGER.get_context()
  rdd = _get_kronos_rdd(spark_context, stream_in, start_time, end_time)
  for json_transform in transforms:
    metis_transform = transform.parse(json_transform)
    rdd = metis_transform.apply(spark_context, rdd)
  result = rdd.collect()
  CONTEXT_MANAGER.release_context(spark_context)
  if stream_out is not None:
    min_time = min(event['time'] for event in result)
    max_time = max(event['time'] for event in result)
    # TODO(usmanm): Delete events in `stream_out` from `min_time` to `max_time`.
    KRONOS.put({stream_out: result})
  return result

async_execute_compute_task = async(execute_compute_task)
