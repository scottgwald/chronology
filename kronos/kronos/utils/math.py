from datetime import datetime
from dateutil.tz import tzutc


# Kronos time is the number of 100ns intervals since the UTC epoch.
def time_to_kronos_time(time):
  """
  Takes a unix timestamp or a datetime object and returns a Kronos timestamp.
  """
  if isinstance(time, datetime):
    time = (time.replace(tzinfo=tzutc()) -
              datetime.utcfromtimestamp(0)).total_seconds()
  return int(float(time) * 1e7)


def kronos_time_to_datetime(time, round_up=False):
  time = int(time / 1e7)
  if round_up:
    time += 1
  return datetime.utcfromtimestamp(time)


def kronos_time_to_time(kronos_time):
  return float(kronos_time) / 1e7


def round_down(value, base):
  """
  Round `value` down to the nearest multiple of `base`.
  Expects `value` and `base` to be non-negative.
  """
  return int(value - (value % base))
