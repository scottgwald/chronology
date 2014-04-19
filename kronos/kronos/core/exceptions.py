class BackendMissing(Exception):
  pass

class ConnectionFailure(Exception):
  pass

class InvalidRequest(Exception):
  pass

class InvalidEventId(Exception):
  pass

class InvalidEventTime(Exception):
  pass

class InvalidStreamName(Exception):
  pass

class ImproperlyConfigured(Exception):
  pass

class InvalidBucketIntervalComparison(Exception):
  pass

class InvalidUUIDComparison(Exception):
  pass

class NamespaceMissing(Exception):
  pass
