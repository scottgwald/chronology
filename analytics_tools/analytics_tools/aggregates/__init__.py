from analytics_tools.aggregates.analysis import aggregate_stream

class Aggregator(object):
  """
  After creating an Aggregator object, you call `update` on it as many
  times as you would like, and call `aggregate` once you'd like the
  aggregate computed
  """
  def update(self, value):
    raise NotImplementedError('Please implement `update`')

  def aggregate(self):
    raise NotImplementedError('Please implement `aggregate`')


class Count(Aggregator):
  def __init__(self):
    self._count = 0
    
  def update(self, value):
    self._count += 1

  def aggregate(self):
    return self._count


AGGREGATORS = {
  'count': Count
}
