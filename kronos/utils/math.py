from uuid import (uuid4, UUID)

def uuid_to_unix_time(uuid):
  """
  UUIDs contain a time field. Convert it to unix time and return.
  """

  if not isinstance(uuid, UUID):
    raise Exception("Expected type UUID")
  return (uuid.time - 0x01b21dd213814000L) * 100 / 1e9

def uuid_from_time(time):
  """
  Generate a UUID with the specified time.
  """

  # Bit-flipping logic from uuid1 implementation described in:
  # http://stackoverflow.com/questions/7153844/uuid1-from-utc-timestamp-in-python
  # except we use a random UUID to seed the clock sequence to minimize the
  # probability of two calls to this function with the same time getting the
  # same ID.
  nanoseconds = int(time * 1e9)
  timestamp = int(nanoseconds/100) + 0x01b21dd213814000L
  time_low = timestamp & 0xffffffffL
  time_mid = (timestamp >> 32L) & 0xffffL
  time_hi_version = (timestamp >> 48L) & 0x0fffL
  randomuuid = uuid4()
  uuid = UUID(fields = (time_low,
                        time_mid,
                        time_hi_version,
                        randomuuid.clock_seq_hi_variant,
                        randomuuid.clock_seq_low,
                        randomuuid.node),
              version = 1)
  return uuid

def round_down(value, base):
    """
    Round `value` down to the nearest multiple of `base`.
    Expects `value` and `base` to be non-negative.
    """
    return int(value - (value % base))
