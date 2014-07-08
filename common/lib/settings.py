import copy
import imp
import json


def merge_dicts(dict1, dict2, make_copy=True):
  """ Recursively merges dict2 with dict1. Otherwise if both dict1 and dict2
  have a key, it is overwritten by the value in dict2. """
  if make_copy:
    dict1 = copy.copy(dict1)
  for key, value in dict2.iteritems():
    if isinstance(dict1.get(key), dict) and isinstance(value, dict):
      value = merge_dicts(dict1[key], value, make_copy=False)
    dict1[key] = value
  return dict1


# TODO(usmanm): Figure out funky behaviour when keys have periods in them.
class AttributeProxyDict(dict):
  def __getattr__(self, attr):
    value = self[attr]
    if isinstance(value, dict) and not isinstance(value, AttributeProxyDict):
      self[attr] = value = AttributeProxyDict(value)
    return value

  def __setattr__(self, attr, value):
    self[attr] = value

  def __getitem__(self, item):
    parts = item.split('.', 1)
    key = parts[0]
    parts = parts[1:]
    
    value = super(AttributeProxyDict, self).__getitem__(key)
    if parts:
      value = value[parts[0]]
    return value


class Settings(AttributeProxyDict):
  @staticmethod
  def dictify(obj):
    if isinstance(obj, dict):
      return obj
    _dict = {}
    for attr in dir(obj):
      if attr.startswith('_'): # Ignore private fields.
        continue
      _dict[attr] = getattr(obj, attr)
    return _dict

  def update_from_file(self, path):
    with open(path) as f:
      content = f.read()
    if path.endswith('.py'):
      self.update(imp.load_source('chronology.common.settings.temp', path))
    elif path.endswith('.json'):
      self.update(json.loads(content))

  def update(self, source):
    attrs = merge_dicts(self, Settings.dictify(source))
    self.clear()
    super(Settings, self).update(attrs)
