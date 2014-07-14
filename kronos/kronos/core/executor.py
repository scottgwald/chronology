from kronos.common.concurrent import AbstractExecutor
from kronos.common.concurrent import GIPCExecutor
from kronos.common.concurrent import GreenletExecutor
from kronos.conf import settings

_GIPC_EXECUTOR = None
_GREENLET_EXECUTOR = None


def execute_greenlet_async(func, *args, **kwargs):
  """
  Executes `func` in a separate greenlet in the same process. Memory and other
  resources are available (e.g. TCP connections etc.) `args` and `kwargs` are
  passed to `func`.
  """
  global _GREENLET_EXECUTOR
  if _GREENLET_EXECUTOR is None:
    _GREENLET_EXECUTOR = GreenletExecutor(
      num_greenlets=settings.node.greenlet_pool_size)
  return _GREENLET_EXECUTOR.submit(func, *args, **kwargs)


def execute_process_async(func, *args, **kwargs):
  """
  Executes `func` in a separate process. Memory and other resources are not
  available. This gives true concurrency at the cost of losing access to
  these resources. `args` and `kwargs` are
  """
  global _GIPC_EXECUTOR
  if _GIPC_EXECUTOR is None:
    _GIPC_EXECUTOR = GIPCExecutor(
      num_procs=settings.node.gipc_pool_size,
      num_greenlets=settings.node.greenlet_pool_size)
  return _GIPC_EXECUTOR.submit(func, *args, **kwargs)


def wait(results, num=None, timeout=None):
  """
  Wait for results of async executions to become available/ready.
  
  `results`: List of AsyncResult instances returned by one of
             `execute_greenlet_async` or `execute_process_async`.
  `num`: Number of results to wait for. None implies wait for all results.
  `timeout`: Number of seconds to wait for `num` of the `results` to become
             ready.
  """
  return AbstractExecutor.wait(results, num=num, timeout=timeout)
