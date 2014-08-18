class Node(object):
  """
  Represents a node in the query plan structure. Provides a `parse` function to
  parse JSON into a heirarchy of nodes. Executors take a plan consisting of
  nodes and use it to apply the Transformations to the stream.
  """
  @classmethod
  def parse(cls, _dict):
    raise NotImplemented

  def to_dict(self):
    _dict = {}
    for key in dir(self):
      if (key.startswith('_') or key.lower() != key):
        continue
      value = getattr(self, key)
      if (callable(value) or value is None or value is False or value == []):
        continue
      if isinstance(value, Node):
        value = value.to_dict()
      if isinstance(value, list):
        for i, v in enumerate(value):
          if isinstance(v, Node):
            value[i] = v.to_dict()
      _dict[key] = value
    return _dict

  def __eq__(self, other):
    if not isinstance(other, Node):
      return False
    return self.to_dict() == other.to_dict()


class ExecutableNode(Node):
  pass
