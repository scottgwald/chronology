import types

from metis.core.query import ExecutableNode
from metis.utils.enum import Enum


class Stream(ExecutableNode):
  class Type(Enum):
    KRONOS = 'kronos'

  def __init__(self, alias=None):
    self.alias = alias

  @classmethod
  def parse(cls, _dict):
    typ = _dict.pop('type')
    assert typ in Stream.Type.values()

    if typ == Stream.Type.KRONOS:
      return KronosStream.parse(_dict)


class KronosStream(Stream):
  def __init__(self, host, stream, start_time, end_time, namespace=None,
               **kwargs):
    self.type = Stream.Type.KRONOS
    self.host = host
    self.stream = stream
    self.start_time = start_time
    self.end_time = end_time
    self.namespace = namespace
    super(KronosStream, self).__init__(**kwargs)

  @classmethod
  def parse(cls, _dict):
    kronos_stream = KronosStream(**_dict)

    # TODO(usmanm): Improve validation.
    assert isinstance(kronos_stream.host, types.StringTypes)
    assert isinstance(kronos_stream.stream, types.StringTypes)
    assert isinstance(kronos_stream.start_time, int)
    assert isinstance(kronos_stream.end_time, int)
    assert (kronos_stream.namespace is None or
            isinstance(kronos_stream.namespace, types.StringTypes))

    return kronos_stream
