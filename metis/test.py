#!/usr/bin/python

"""
Integration test for Metis.  Usage:

  python test.py --kronos-url http://localhost:8151 --metis-url http://localhost:8152|less
"""

from argparse import ArgumentParser
from pykronos import KronosClient

import json
import requests

def generate_data(client, test_stream, seconds):
  group1_options = ['whale', 'seagull', 'crab', 'seal', 'squid']
  group2_options = ['angela', 'barack']
  group3_options = ['mandela', 'king', 'gandhi']
  value_options = [1.0 * value for value in xrange(10)]

  start = 0
  end = seconds - 1
  client.delete(test_stream, start, end + 1)
  for idx in xrange(seconds):
    blob = {
      '@time': idx,
      'group1': group1_options[idx % len(group1_options)],
      'group2': group2_options[idx % len(group2_options)],
      'group3': group3_options[idx % len(group3_options)],
      'value': value_options[idx % len(value_options)]
    }
    client.put({test_stream: [blob]})
    

def make_request(client, metis_url, test_stream, seconds):
  transforms = [
                 {'key': 'group2', 'value': 'angela', 'op': 'eq',
                  'transform': 'FILTER'},
                 {'keys': ['@id', '@time', 'group1', 'group2',
                           'group3', 'value'],
                  'transform': 'PROJECTION'},
                 {'time_width': 100, 'transform': 'GROUPBYTIME'},
                 {'keys': ['group1'], 'transform': 'GROUPBY'},
                 {'aggregates': [{'alias': 'sum_of_values', 'key': 'value',
                                  'op': 'sum'}], 'transform': 'AGGREGATE'}
    
               ]
  """                """                
  response = requests.post('%s/1.0/events/get' % metis_url,
                           data=json.dumps({'stream': test_stream,
                                            'start_time': 0,
                                            'end_time': seconds,
                                            'transforms': transforms}),
                           stream=True)
  print response.json()['result']
  
if __name__ == '__main__':
  parser = ArgumentParser(description='Kronos test runner.')
  parser.add_argument('--kronos-url', required=True)
  parser.add_argument('--metis-url', required=True)
  parser.add_argument('--test-stream', default='metis.test.raw')
  parser.add_argument('--test-seconds', type=int, default=1000)
  args = parser.parse_args()

  client = KronosClient(args.kronos_url, blocking=False)
  generate_data(client, args.test_stream, args.test_seconds)
  client.flush()
  make_request(client, args.metis_url, args.test_stream, args.test_seconds)
  
