import threading

from metis import app
from metis.core.query.operator import Operator
from metis.utils.decorators import async

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
    from pyspark import SparkContext
    context = SparkContext(app.config['SPARK_MASTER'],
                           'Metis-%s' % self._contexts_created,
                           pyFiles=[app.config['METIS_LIB_FILE']])
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


def execute_compute_task(plan):
  spark_context = CONTEXT_MANAGER.get_context()
  try:
    result = Operator.parse(plan).get_rdd(spark_context).collect()
  finally:
    CONTEXT_MANAGER.release_context(spark_context)
  return result

async_execute_compute_task = async(execute_compute_task)
