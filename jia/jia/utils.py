def get_seconds(value, scale):
  """Convert time scale dict to seconds

  Given a dictionary with keys for scale and value, convert
  value into seconds based on scale.
  """
  scales = {
    'seconds': lambda x: x,
    'minutes': lambda x: x * 60,
    'hours': lambda x: x * 60 * 60,
    'days': lambda x: x * 60 * 60 * 24,
    'weeks': lambda x: x * 60 * 60 * 24 * 7,
    'months': lambda x: x * 60 * 60 * 24 * 30,
    'years': lambda x: x * 60 * 60 * 24 * 365,
  }

  return scales[scale](value)

