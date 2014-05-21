def get_property(event, name):
  """
  Gets a property from `event` using a dot-notation name `name`. It's preferable
  that dots are not used in key names but this function tries to do the right
  thing by trying all combinations of property names. It prefers property
  names with dots over nested dicts. For example if the dict looks like:
    { 'a': { 'b': 1 },
      'a.b': 2 }
  then this function will return 2 if `name` is 'a.b' rather than 1.
  """
  if isinstance(name, list):
    parts = name
  else:
    parts = name.split('.')
  for i in xrange(len(parts), 0, -1):
    name = '.'.join(parts[:i])
    if not name in event:
      continue
    value = event[name]
    if i == len(parts):
      return value
    elif isinstance(value, dict):
      return get_property(value, parts[i:])
    raise KeyError
  else:
    raise KeyError
  return parts
