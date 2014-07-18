ERRORS_FIELD = '@errors'
ID_FIELD = '@id'
SUCCESS_FIELD = '@success'
TIMESTAMP_FIELD = '@time'
TOOK_FIELD = '@took'

MAX_LIMIT = 2**31 - 1 # Max 32-bit signed int.
SETTINGS_PATH = '/etc/kronos/settings.py'


class ResultOrder(object):
  ASCENDING = 'ascending'
  DESCENDING = 'descending'


class ServingMode(object):
  ALL = 'all'
  COLLECTOR = 'collector'
  READONLY = 'readonly'
