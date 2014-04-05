import functools

from flask import g
from flask import redirect
from flask import request
from flask import url_for
from flask_googleauth import GoogleAuth
from jia import app

auth = GoogleAuth(app)

ERROR_MESSAGE = """
User {} is not allowed.
<a href="https://security.google.com/settings/security/permissions">
Revoke access on this account before trying again</a>.
"""

def authenticate(fn):
  @functools.wraps(fn)
  def decorated(*args, **kwargs):
    if app.config['ENABLE_GOOGLE_AUTH']:
      authenticated = False
      if g.user:
        for pattern in app.config['ALLOWED_EMAILS']:
          if pattern.match(g.user['email']):
            authenticated = True
            break
        if not authenticated:
          auth._logout()
          return ERROR_MESSAGE.format(g.user['email'])
      else:
        return redirect(url_for("%s.login" % auth.blueprint.name,
                                next=request.url))
    return fn(*args, **kwargs)
  return decorated
