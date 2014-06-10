import functools
import urllib
import requests

from flask import redirect
from flask import request
from flask import url_for
from flask import session
from jia import app

auth_uri = 'https://accounts.google.com/o/oauth2/auth'
token_uri = 'https://accounts.google.com/o/oauth2/token'
scope = ('https://www.googleapis.com/auth/userinfo.profile',
         'https://www.googleapis.com/auth/userinfo.email')
profile_uri = 'https://www.googleapis.com/oauth2/v1/userinfo'

ERROR_MESSAGE = """
User {} is not allowed.
<a href="https://security.google.com/settings/security/permissions">
Revoke access on this account before trying again</a>.
"""

def require_auth(fn):
  @functools.wraps(fn)
  def decorated(*args, **kwargs):
    if app.config['ENABLE_GOOGLE_AUTH']:
      authenticated = False
      if 'email' in session:
        for pattern in app.config['ALLOWED_EMAILS']:
          if pattern.match(session['email']):
            authenticated = True
            break
        if not authenticated:
          session.pop('email', '')
          return ERROR_MESSAGE.format(g.user['email'])
      else:
        params = dict(response_type='code',
                      scope=' '.join(scope),
                      client_id=app.config['GOOGLE_CLIENT_ID'],
                      approval_prompt='auto',
                      redirect_uri=url_for('google_callback', _external=True))
        url = auth_uri + '?' + urllib.urlencode(params)
        session['next'] = request.path
        return redirect(url)
    return fn(*args, **kwargs)
  return decorated


@app.route('/google_callback')
def google_callback():
  if 'code' in request.args:
    code = request.args.get('code')
    data = dict(code=code,
                client_id=app.config['GOOGLE_CLIENT_ID'],
                client_secret=app.config['GOOGLE_CLIENT_SECRET'],
                redirect_uri=url_for('google_callback', _external=True),
                grant_type='authorization_code')
    r = requests.post(token_uri, data=data)
    access_token = r.json()['access_token']
    r = requests.get(profile_uri, params={'access_token': access_token})
    session['email'] = r.json()['email']
    redirect_to = session.pop('next', url_for('index'))
    return redirect(redirect_to)
  else:
    return 'ERROR'
