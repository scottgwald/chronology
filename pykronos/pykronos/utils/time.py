from datetime import datetime
from dateutil.tz import tzutc

# A timezone aware datetime object representing the UTC epoch.
EPOCH = datetime.utcfromtimestamp(0).replace(tzinfo=tzutc())

def datetime_to_kronos_time(dt):
  """
  Kronos expects timestamps to be the number of 100ns intervals since the epoch.
  This function takes a Python `datetime` object and returns the corresponding
  Kronos timestamp. If `dt` is a native datetime object (not timezone aware)
  then this function assumes that the timezone in UTC.
  """
  # If the datetime is native, assume that the timezone is UTC.
  if not dt.tzinfo:
    dt = dt.replace(tzinfo=tzutc())
  return int((dt - EPOCH).total_seconds() * 1e7)

def kronos_time_to_datetime(time):
  return datetime.utcfromtimestamp(int(time * 1e-7)).replace(tzinfo=tzutc())

def kronos_time_now():
  return datetime_to_kronos_time(datetime.now(tz=tzutc()))

def is_kronos_reserved_key(key):
  return len(key) and key.startswith('@')
