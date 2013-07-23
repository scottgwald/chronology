from datetime import datetime

# A timezone aware datetime object representing the UTC epoch.
EPOCH = datetime.utcfromtimestamp(0)

def datetime_to_kronos_time(dt):
  """
  Kronos expects timestamps to be the number of 100ns intervals since the epoch.
  This function takes a Python `datetime` object and returns the corresponding
  Kronos timestamp.
  """
  return int((dt - EPOCH).total_seconds() * 1e7)

def kronos_time_now():
  return datetime_to_kronos_time(datetime.utcnow())
