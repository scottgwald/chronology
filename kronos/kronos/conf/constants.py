ID_FIELD = '@id'
TIMESTAMP_FIELD = '@time'

MAX_LIMIT = 2**31 - 1 # Max 32-bit signed int.
SETTINGS_PATH = '/etc/kronos/settings.py'


class ResultOrder(object):
  ASCENDING = 'ascending'
  DESCENDING = 'descending'


class ServingMode(object):
  ALL = 'all'
  COLLECTOR = 'collector'
  READONLY = 'readonly'
