from __future__ import absolute_import

import datetime
import gevent
import gipc
import traceback

from heapq import heappush, heappop, heapify
from common.concurrent import GIPCExecutor
from scheduler.models import Task

class Scheduler(object):
  """Inteval based code execution scheduler"""

  def __init__(self):
    """Initialize the queue and spawn the main loop thread

    Upon initialization, tasks stored in the database are immediately
    scheduled.

    _task_queue is a priority queue ordered using Python's heapq functionality.
    Elements in _task_queue are tuples of the form (datetime, task) where
    datetime is the scheduled run time and task is a dictionary as defined
    in the above docstring for the Scheduler class.

    For concurrency safety reasons, never write to _task_queue outside the
    _loop() thread.
    """
    self._task_queue = []  # Never write to this outside the _loop thread
    self._pending_cancels = set()
    self._executor = GIPCExecutor()

    # Load previously scheduled tasks from database
    now = datetime.datetime.now()
    saved_schedule = Task.query.filter_by(active=True)
    for task in saved_schedule:
      new_task = {
        'id': task.id,
        'interval': task.interval,
        'code': task.code
      }
      # Writing directly to the _task_queue is safe since we haven't started
      # the _loop yet
      self._task_queue.append((now, new_task))

    # Make _task_queue a priority queue
    heapify(self._task_queue)

    # Spawn main loop and save writer for future communication
    (read, write) = gipc.pipe()
    gevent.spawn(self._loop, read)
    self._schedule_pipe = write

  def schedule(self, task):
    """Pass schedule request to the main loop

    Tasks should be dictionaries with the following attributes:

    task = {
      'id': 'a93de0f3',
      'code': ...,  # string of Python code
      'interval': 600,  # in seconds
    }

    An interval of 0 indicates the task should only be run once.
    """
    self._schedule_pipe.put(('schedule', task))

  def cancel(self, task_id):
    """Pass cancel request to the main loop"""
    self._schedule_pipe.put(('cancel', task_id))

  def _schedule(self, task, next_run=None):
    now = datetime.datetime.now()
    if not next_run:
      next_run = datetime.datetime.now()
    heappush(self._task_queue, (next_run, task))

  def _cancel(self, task_id):
    self._pending_cancels.add(task_id)

  def _loop(self, reader):
    """Main execution loop of the scheduler.

    The loop runs every second. Between iterations, the loop listens for
    schedule or cancel requests coming from Flask via over the gipc pipe
    (reader) and modifies the queue accordingly.

    When a task completes, it is rescheduled
    """
    results = set()

    while True:
      now = datetime.datetime.now()
      if self._task_queue and self._task_queue[0][0] <= now:
        task = heappop(self._task_queue)[1]
        if task['id'] not in self._pending_cancels:
          result = self._executor.submit(_execute, task)
          results.add(result)
        else:
          self._pending_cancels.remove(task['id'])
      else:
        # Check for new tasks coming from HTTP
        with gevent.Timeout(0.5, False) as t:
          message = reader.get(timeout=t)
          if message[0] == 'schedule':
            self._schedule(message[1], next_run=now)
          elif message[0] == 'cancel':
            self._cancel(message[1])
        # Reschedule completed tasks
        if not results:
          gevent.sleep(0.5)
          continue
        ready = self._executor.wait(results, num=1, timeout=0.5)
        for result in ready:
          results.remove(result)
          if result.value:
            task = result.value
            interval = int(task['interval'])
            if interval:
              run_at = now + datetime.timedelta(seconds=int(task['interval']))
              self._schedule(task, next_run=run_at)
          else:
            print "ERROR:", result.exception

def _execute(task):
  """A wrapper around exec

  This exists outside the Scheduler class because it is pickled after it is
  sent to the executor.

  TODO(derek): add better exception handling
  TODO(derek): if the code being `exec`ed modifies the variable `task` (or
  presumably other things in scope) everything gets messed up
  """
  print "[%s] -- %s -- START" % (datetime.datetime.now(), task['id'])
  try:
    exec task['code']
    print "[%s] -- %s -- COMPLETE" % (datetime.datetime.now(), task['id'])
  except Exception as e:
    print "[%s] -- %s -- FAIL" % (datetime.datetime.now(), task['id'])
    print traceback.format_exc()
  finally:
    return task
