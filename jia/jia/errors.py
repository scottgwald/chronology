import json
import traceback

from flask import make_response


class JiaError(Exception):
  def __init__(self, code=500, message='Server error', data=None):
    self.code = code
    self.data = data
    super(JiaError, self).__init__(message)

  def to_response(self):
    return make_response(json.dumps({'message': self.message,
                                     'data': self.data}),
                         self.code,
                         {'Content-Type': 'application/json'})


class PyCodeError(JiaError):
  def __init__(self, exception, tb):
    if isinstance(exception, SyntaxError):
      message = '%s: %s' % (str(exception), exception.text)
    else:
      message = exception.message
    super(PyCodeError, self).__init__(
      code=400,
      message='An uncaught exception was raised.',
      data={'name': exception.__class__.__name__,
            'message': unicode(message),
            'traceback': traceback.format_tb(tb)})
