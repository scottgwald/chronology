import json as _json
import msgpack
import sys
import ujson

class json(object):
  # ujson has superior decoding performance and produces the same output as
  # json.loads would. We use it in pykronos to speed up event parsing, use it
  # here to speed up request body parsing.
  loads = staticmethod(lambda s: ujson.loads(s, precise_float=True))
  dumps = staticmethod(_json.dumps)

def get_marshaler(name):
  # This is convenient because to support other marshaling libraries, we mostly
  # will just have to add an import statement for them at the top. This is
  # because most Python marshaling libraries support the loads/dumps calls.
  # For example if we want to support msgpack, all we need to do is add an
  # `import msgpack` statement at the top and then call
  # `set_marshaler('msgpack') below.
  return globals()[name]

def set_marshaler(name):
  marshaler = get_marshaler(name)
  setattr(sys.modules[__name__], 'loads', marshaler.loads)
  setattr(sys.modules[__name__], 'dumps', marshaler.dumps)

set_marshaler('msgpack')
