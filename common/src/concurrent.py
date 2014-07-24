import atexit
import functools
import gipc
import itertools
import logging
import multiprocessing
import pickle
import signal

from . import cloudpickle
from gevent import Greenlet
from gevent import wait
from gevent.event import AsyncResult
from gevent.event import Event
from gevent.hub import LoopExit
from gevent.pool import Pool
from gevent.queue import Queue

log = logging.getLogger(__name__)

"""
This module provides a high-level interface for executing callables
asynchronously. Asynchronous execution can be performed with Greenlets (using
GreenletExecutor) or in separate processes (using GIPCExecutor).

Example usage:

  def func(name):
    return open(name).read()

  executor = GreenletExecutor(num_greenlets=5)
  results = []
  # All 5 files files will be read asynchronously and the submit call will not
  # block.
  for name in ['lol', 'cat', 'foo', 'bar', 'baz']:
    results.append(executor.submit(func, [name]))
  # Block till all 5 files have been read.
  executor.wait(results)
  for result in results:
    print result.get()
  executor.shutdown()
"""

# Monkey patch the default encoder for GIPC, so we can serialize functions
# and pass them across IPC.
def _gipc_encoder(obj):
  return cloudpickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
gipc.gipc._default_encoder = _gipc_encoder


class Task(object):
  def __init__(self, _id, func, args, kwargs):
    self.id = _id
    self.result = AsyncResult()
    self.func = func
    self.args = args
    self.kwargs = kwargs

  def execute(self):
    try:
      self.result.set(self.func(*self.args, **self.kwargs))
    except Exception, e:
      log.exception('Task.execute: execution failed.')
      self.result.set_exception(e)


class AbstractExecutor(object):
  """
  An abstract class that provides methods to execute calls asynchronously. It
  should not be used directly, but through its concrete subclasses.
  """
  def __init__(self):
    self.current_task_id = 0
    self.running = True
    atexit.register(self.shutdown)

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.shutdown()

  def _create_task(self, func, args, kwargs):
    task_id = self.current_task_id
    self.current_task_id += 1
    return Task(task_id, func, args, kwargs)
  
  def async(self, func):
    """
    Decorator that will execute `func` asynchronously using this Executor and
    return an AsyncResult object.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
      return self.submit(func, *args, **kwargs)
    return wrapper

  def submit(self, func, *args, **kwargs):
    """
    Schedules the callable, func, to be executed asynchronously as
    func(*args **kwargs) and returns an gevent.event.AsyncResult object
    representing the execution of the callable.
    """
    if not self.is_running():
      raise RuntimeError('Executor instance has been shutdown.')
    task = self._create_task(func, args, kwargs)
    self._submit(task)
    return task.result

  def is_running(self):
    return self.running

  def shutdown(self):
    """
    Signal the executor that it should free any resources that it is using
    when the currently pending callables are done executing. Calls to
    Executor.submit() made after shutdown will raise RuntimeError.
    """
    if not self.is_running():
      return
    self.running = False
    self._shutdown()

  @classmethod
  def wait(cls, results, num=None, timeout=None):
    """
    Blocks till `num` of the results are ready. If `num` is None, then blocks
    till all of the results are ready. `results` must be an iterable of
    AsyncResult objects returned by Executor.submit(). `timeout` is specified
    in seconds and is the time for which `wait` will block.
    """
    if not hasattr(results, 'len'):
      results = list(results)
    num = min(num, len(results))
    return wait(objects=results, count=num, timeout=timeout)


class GreenletExecutor(AbstractExecutor):
  """
  GreenletExecutor is an AbstractExecutor subclass that uses a pool of
  greenlets to execute calls asynchronously.

  NOTE: Use this executor for I/O-bound tasks. Since all greenlets are
  multiplexed on a single pthread, do NOT use this for compute-bound
  callables. Try using the GIPCExecutor instead.
  """
  def __init__(self, num_greenlets=50):
    super(GreenletExecutor, self).__init__()
    self.pool = Pool(size=num_greenlets)
    self.task_queue = Queue()
    self.num_ready = 0

  def _shutdown(self):
    for _ in xrange(len(self.pool)):
      self.task_queue.put(None)
    self.pool.join()

  def _worker_loop(self):
    try:
      self.num_ready += 1
      while True:
        self.num_ready -= 1
        task = self.task_queue.get()
        if task is None:
          return
        task.execute()
        self.num_ready += 1
    except:
      pass

  def _submit(self, task):
    self.task_queue.put(task)
    if not self.num_ready and self.pool.free_count():
      self.pool.spawn(self._worker_loop)


class GIPCExecutor(AbstractExecutor):
  """
  GIPCExecutor class is an AbstractExecutor subclass that uses a pool of
  processes to execute calls asynchronously. Within each process, a
  GreenletExecutor is used to execture non-blocking callables.

  NOTE: Use this executor for compute-bound or I/O-bound tasks. All functions
  passed to this executor are pickled using the `cloudpickle` module.
  Only use functions which don't have sockets and/or other process-dependant
  resources in their closure.
  """
  def __init__(self, num_procs=multiprocessing.cpu_count(), num_greenlets=50):
    super(GIPCExecutor, self).__init__()
    self.procs = []
    self.pipes = []
    self.results = {}
    for _ in xrange(num_procs):
      cend, pend = gipc.pipe(duplex=True)
      proc = gipc.start_process(self._proc_loop, args=(cend, num_greenlets),
                                daemon=True)
      self.procs.append(proc)
      self.pipes.append(pend)
    self.pipes = itertools.cycle(self.pipes)
    self.result_pool = self._result_pool()

  def _result_pool(self):
    # TODO(usmanm): Can we reduce this from a Pool to a single Greenlet and
    # use the `wait` function?
    results = self.results
    pool = Pool(size=len(self.procs))
    
    def _read_loop(pipe):
      while True:
        result = pipe.get()
        if result is None:
          return
        _id, successful, value = result
        if successful:
          results[_id].set(value)
        else:
          results[_id].set_exception(value)
        del results[_id]

    for _ in xrange(len(self.procs)):
      pool.spawn(_read_loop, self.pipes.next())

    return pool

  def _proc_loop(self, pipe, num_greenlets):
    result_to_id = {}
    executor = GreenletExecutor(num_greenlets=num_greenlets)
    # Event to indicate that there are some tasks in flight and so write_loop
    # should start waiting on their results.
    tasks_in_flight = Event()
    
    def on_kill(signum, frame):
      pipe.put(None)
      return

    signal.signal(signal.SIGINT, on_kill) # When server is terminated.

    def signal_tasks_in_flight():
      if not tasks_in_flight.is_set():
        tasks_in_flight.set()

    def read_loop():
      """
      Reads all tasks sent across the pipe by the parent process and passes
      them to the local GreenletExecutor instance.
      """
      while True:
        task_args = pipe.get()
        if task_args is None:
          executor.shutdown()
          signal_tasks_in_flight()
          return
        task = Task(*task_args)
        result_to_id[task.result] = task.id
        executor._submit(task)
        signal_tasks_in_flight()
        
    read_greenlet = Greenlet.spawn(read_loop)

    def write_loop():
      """
      Waits for results of tasks being executed by the local GreenletExecutor
      and pipes the result back to the parent (callee) process.
      """
      while True:
        if not result_to_id:
          # Wait for some result to be ready.
          tasks_in_flight.clear()
          try:
            if read_greenlet.ready():
              raise LoopExit
            tasks_in_flight.wait()
            continue
          except LoopExit, e:
            pipe.put(None)
            return
        ready_results = wait(objects=result_to_id.keys(), count=1)
        for result in ready_results:
          try:
            value = result.get()
          except Exception, e:
            value = e
          pipe.put((result_to_id[result], result.successful(), value))
          del result_to_id[result]

    write_loop()

  def _shutdown(self):
    for _ in xrange(len(self.procs)):
      pipe = self.pipes.next()
      pipe.put(None)
    self.result_pool.join()
    for proc in self.procs:
      proc.join()
  
  def _submit(self, task):
    pipe = self.pipes.next()
    self.results[task.id] = task.result
    pipe.put((task.id, task.func, task.args, task.kwargs))
