import json
import requests
import unittest

from metis import app
from pykronos import KronosClient

class MetisServerTestCase(unittest.TestCase):
  '''
  Unit tests for all available `Operator` types.
  '''
  def setUp(self):
    self.kronos_client = KronosClient(app.config['KRONOS_SERVER'])
    self.index_path = '1.0/index'
    self.query_path = '1.0/query'
    self.server_url = 'http://localhost:9192/%s'

  def index(self):
    response = requests.get(self.server_url % self.index_path)
    self.assertEqual(response.status_code, requests.codes.ok)
    return response.json()

  def query(self, plan):
    response = requests.post(self.server_url % self.query_path,
                             data=json.dumps({'plan': plan}))
    self.assertEqual(response.status_code, requests.codes.ok)
    events = []
    for line in response.text.split('\n'):
      line = line.strip()
      if not line:
        continue
      events.append(json.loads(line))
    return events
