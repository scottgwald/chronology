from metis.core.query.stream import KronosStream
from metis.core.query.transform import Aggregate
from metis.core.query.transform import Filter
from metis.core.query.transform import Join
from metis.core.query.transform import Limit
from metis.core.query.transform import OrderBy
from metis.core.query.transform import Project

# XXX: Whenever adding a new `Stream`, `Transform` or a subclass of
# `ExecutableNode`, please add a method for to the `Executor` class below and
# create a dispatch case inside `Executor.execute`.

class Executor(object):
  """
  The Executor interface. ALL methods must be copied over (and implemented)
  to an Executor implementation, including the first method below which sets
  up the dynamic dispatcher.
  """  
  def execute(self, node):
    if isinstance(node, KronosStream):
      return self.execute_kronos_stream(node)

    if isinstance(node, Aggregate):
      return self.execute_aggregate(node)
    if isinstance(node, Filter):
      return self.execute_filter(node)
    if isinstance(node, Join):
      return self.execute_join(node)
    if isinstance(node, Limit):
      return self.execute_limit(node)
    if isinstance(node, OrderBy):
      return self.execute_order_by(node)
    if isinstance(node, Project):
      return self.execute_project(node)

    raise NotImplemented

  def finalize(self, result):
    return result

  def execute_kronos_stream(self, node):
    raise NotImplemented

  def execute_aggregate(self, node):
    raise NotImplemented

  def execute_filter(self, node):
    raise NotImplemented

  def execute_join(self, node):
    raise NotImplemented

  def execute_limit(self, node):
    raise NotImplemented

  def execute_order_by(self, node):
    raise NotImplemented

  def execute_project(self, node):
    raise NotImplemented
