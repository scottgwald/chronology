class Enum(object):
  @classmethod
  def values(cls):
    if hasattr(cls, '_values'):
      return cls._values
    values = set()
    for _key, value in cls.__dict__.iteritems():
      if _key == _key.upper():
        values.add(value)
    cls._values = values
    return values
