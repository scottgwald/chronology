VERSION = (0, 5, 'alpha')

def get_version(version=None):
  version = version or VERSION
  assert(len(version) == 3)
  return '%s.%s %s' % version
