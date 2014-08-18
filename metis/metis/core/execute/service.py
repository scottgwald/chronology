from importlib import import_module

from metis import app
from metis.common.lazy import LazyObjectMetaclass
from metis.core.query.plan import parse


class ExecutorService(object):
  __metaclass__ = LazyObjectMetaclass

  def __init__(self):
    executor_path = 'metis.core.execute.%s' % app.config['EXECUTOR']
    executor_module, executor_cls = executor_path.rsplit('.', 1)
    executor_module = import_module(executor_module)
    executor_cls = getattr(executor_module, executor_cls)
    self.executor = executor_cls()

  def execute_plan(self, plan):
    return self.executor.finalize(self.executor.execute(parse(plan)))


service = ExecutorService()
