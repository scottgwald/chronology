from timeuuid import timeuuid_from_time
from timeuuid import UUIDType

UUID_TIME_OFFSET = 0x01b21dd213814000L


def uuid_to_kronos_time(uuid):
  """
  UUIDs contain a time field. Convert it to kronos time and return.
  """
  return uuid.time - UUID_TIME_OFFSET


def uuid_from_kronos_time(time, _type=UUIDType.RANDOM):
  """
  Generate a UUID with the specified time.
  If `lowest` is true, return the lexicographically first UUID for the specified
  time.
  """
  return timeuuid_from_time(int(time) + UUID_TIME_OFFSET, type=_type)


LOWEST_UUID = timeuuid_from_time(0, UUIDType.LOWEST)
HIGHEST_UUID = timeuuid_from_time(2**60 - 1, UUIDType.HIGHEST)
