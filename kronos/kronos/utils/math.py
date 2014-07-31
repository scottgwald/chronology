def round_down(value, base):
  """
  Round `value` down to the nearest multiple of `base`.
  Expects `value` and `base` to be non-negative.
  """
  return int(value - (value % base))
