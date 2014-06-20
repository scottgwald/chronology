#!/usr/bin/python
# coding: utf-8
"""
A script to list the available streams on a Kronos server.

example usage:
  python scripts/list_streams.py --kronos-url http://your-kronos.instance:port [--namespace some_namespace]
"""

import argparse
import logging
import time

from kronos.utils.math import time_to_kronos_time
from pykronos import KronosClient
from pykronos.errors import KronosClientError

log = logging.getLogger(__name__)

def check_latency(client, namespace, stream, timeout):
  try:
    list(client.get(stream,
                    0,
                    time_to_kronos_time(time.time()),
                    limit=1,
                    namespace=namespace,
                    timeout=timeout / 1000.0))
    return True
  except KronosClientError:
    return False

def main(args):
  client = KronosClient(args.kronos_url)
  for stream in client.get_streams(namespace=args.namespace):
    if args.read_latency is None or check_latency(client,
                                                  args.namespace,
                                                  stream,
                                                  args.read_latency):
      print stream

def process_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--kronos-url',
                      required=True,
                      help='The Kronos server to retrieve data from')
  parser.add_argument('--namespace',
                      help='The namespace to read from (optional)')
  parser.add_argument('--read-latency',
                      type=int,
                      help=('Only return streams where read latency is at most '
                            'this value. Specify in ms. (optional).'))
  args = parser.parse_args()
  return args


if __name__ == '__main__':
  main(process_args())
