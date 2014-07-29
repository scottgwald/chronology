import hmac
import json

def create_token(key, payload):
  """Auth token generator

  payload should be a json encodable data structure
  """

  token = hmac.new(key)
  token.update(json.dumps(payload))
  return token.hexdigest()

