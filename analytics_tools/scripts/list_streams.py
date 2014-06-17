#!/usr/bin/python
# coding: utf-8
"""
A script to list the available streams on a Kronos server.

example usage:
  python scripts/list_streams.py --kronos-url http://your-kronos.instance:port [--namespace some_namespace]
"""

import argparse
import logging
from pykronos import KronosClient

log = logging.getLogger(__name__)

def main(args):
  client = KronosClient(args.kronos_url)
  for stream in client.get_streams(namespace=args.namespace):
    print stream

def process_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--kronos-url',
                      required=True,
                      help='The Kronos server to retrieve data from')
  parser.add_argument('--namespace',
                      help='The namespace to read from (optional)')
  args = parser.parse_args()
  return args


if __name__ == '__main__':
  main(process_args())
