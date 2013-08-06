import re
from urlparse import urljoin
from urlparse import urlparse

from flask import redirect
from flask import request
from flask import session
from functools import wraps
from openid.consumer import consumer
from openid.consumer.discover import OpenIDServiceEndpoint
from openid.extensions import ax
from openid.store.memstore import MemoryStore

from jia import app

AUTH_KEY = 'jia_id'
ALLOWED_USERS = app.config['ALLOWED_USERS']
OPENID_ENDPOINT = OpenIDServiceEndpoint.fromOPEndpointURL(app.config['OPENID_ENDPOINT'])
store = MemoryStore()

def is_user_allowed(user):
  for pattern in ALLOWED_USERS:
    if pattern.match(user):
      return True
  return False

def require_auth(function):
  if not app.config['ENABLE_OPENID']:
    return function

  @wraps(function)
  def wrapper(*args, **kwargs):
    # We stored a cookie on the client, so client has already authenticated.
    if AUTH_KEY in session:
      user_email = session[AUTH_KEY]
      if is_user_allowed(user_email):
        return function(*args, **kwargs)
      else:
        return 'User {} not allowed.'.format(user_email)

    c = consumer.Consumer(session, store)
    auth_request = c.beginWithoutDiscovery(OPENID_ENDPOINT)

    # Getting an email address with OpenID is not standardized, so use the AX
    # extension.
    ax_request = ax.FetchRequest()
    ax_request.add(ax.AttrInfo('http://axschema.org/contact/email', required=True))
    auth_request.addExtension(ax_request)

    # Store the URL the user is currently trying to reach so that we can
    # redirect there on successful authentication.
    session['requested_url'] = request.url

    # The OpenID provider should redirect the user to the `auth` endpoint.
    parsed_url = urlparse(request.url)
    root = '{}://{}'.format(parsed_url.scheme, parsed_url.netloc)
    return_to = urljoin(root, 'auth')

    redirect_url = auth_request.redirectURL(root, return_to)
    return redirect(redirect_url)
  return wrapper

@app.route('/auth')
def auth():
  global store
  c = consumer.Consumer(session, store)
  response = c.complete(request.args.to_dict(), request.base_url)

  if response.status == consumer.SUCCESS:
    try:
      ax_response = ax.FetchResponse.fromSuccessResponse(response)
      user_email = ax_response.get('http://axschema.org/contact/email')[0]
    except:
      return 'Authentication failed; could not get user\'s email address.'

    if is_user_allowed(user_email):
      # Sign the user's email address and store it in a cookie so that on
      # subsequent visits we can avoid contacting the OpenID provider. This is
      # not robust against cookie stealing attacks.
      # TODO(meelap): Persist this cookie across sessions?
      session[AUTH_KEY] = user_email

      # Redirect the user to the URL he was originally trying to access.
      path = session.get('requested_url', None)
      if not path:
        parsed_url = urlparse(request.url)
        path = '{}://{}'.format(parsed_url.scheme, parsed_url.netloc)
      return redirect(path)
    else:
      return 'User {} not allowed.'.format(user_email)
  
  if info.status == consumer.CANCEL:
    return 'Authentication canceled.'

  return 'Authentication failed.'


