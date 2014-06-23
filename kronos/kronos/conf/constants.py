ID_FIELD = '@id'
TIMESTAMP_FIELD = '@time'
MAX_LIMIT = 2**31 - 1 # Max 32-bit signed int.

class ResultOrder(object):
  ASCENDING = 'ascending'
  DESCENDING = 'descending'

  @staticmethod
  def get_multiplier(order):
    return 1 if order == ResultOrder.ASCENDING else -1

  @staticmethod
  def get_short_name(order):
    return 'asc' if order == ResultOrder.ASCENDING else 'desc'

class ServingMode(object):
  ALL = 'all'
  COLLECTOR = 'collector'
  READONLY = 'readonly'
