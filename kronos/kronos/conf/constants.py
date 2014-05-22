ID_FIELD = '@id'
TIMESTAMP_FIELD = '@time'


class ResultOrder(object):
  ASCENDING = 'ascending'
  DESCENDING = 'descending'

  @staticmethod
  def get_multiplier(order):
    return 1 if order == ResultOrder.ASCENDING else -1


class ServingMode(object):
  ALL = 'all'
  COLLECTOR = 'collector'
  READONLY = 'readonly'
