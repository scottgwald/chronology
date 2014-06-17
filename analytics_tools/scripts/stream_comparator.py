#!/usr/bin/python
# coding: utf-8
"""
A script to compare two different streams (from potentially two different
Kronos servers).

example usage:
  python scripts/stream_comparator.py --kronos-url1 http://your-kronos.instance1:port --kronos-url2 http://your-kronos.instance1:port --stream1 stream1_name --stream2 stream2_name --start 2014-06-01T00:00:00 --end 2014-06-02T00:00:00
"""

import argparse
import hashlib
import itertools
import json
import logging
import sys
from pykronos import KronosClient
from pykronos import TIMESTAMP_FIELD

log = logging.getLogger(__name__)


def get_next(events):
  try:
    return events.next()
  except StopIteration:
    return {}


def push_back(event, iterator):
  return itertools.chain([event], iterator)


def main(args):
  client1 = KronosClient(args.kronos_url1, namespace=args.namespace1)
  client2 = KronosClient(args.kronos_url2, namespace=args.namespace2)
  stream1 = client1.get(args.stream1, args.start, args.end)
  stream2 = client2.get(args.stream2, args.start, args.end)

  total_stream1 = 0
  extra_stream1 = 0
  total_stream2 = 0
  extra_stream2 = 0

  # Sorting of events with the same timestamp may vary across backends, hence
  # we can't do a simple loop comparison. We need to aggregate all events with
  # the same timestamp from both streams and then compare the two sets.
  stream1_hashes = set()
  stream2_hashes = set()
  current_timestamp = None
  while True:
    event1 = get_next(stream1)
    event2 = get_next(stream2)
    # Are both streams exhausted?
    if not (event1 or event2):
      break
    # Pick the smaller timestamp from the two events.
    min_timestamp = min(event1.get(TIMESTAMP_FIELD, sys.maxint),
                        event2.get(TIMESTAMP_FIELD, sys.maxint))
    if current_timestamp is None:
      current_timestamp = min_timestamp
    # If min_timestamp is greater than current_timestamp, then aggregate stats
    # for current_timestamp and roll over.
    if min_timestamp > current_timestamp:
      total_stream1 += len(stream1_hashes)
      total_stream2 += len(stream2_hashes)
      extra_stream1 += len(stream1_hashes - stream2_hashes)
      extra_stream2 += len(stream2_hashes - stream1_hashes)
      stream1_hashes.clear()
      stream2_hashes.clear()
      current_timestamp = min_timestamp
    
    if event1:
      assert event1[TIMESTAMP_FIELD] >= current_timestamp
      if event1[TIMESTAMP_FIELD] == current_timestamp:
        stream1_hashes.add(hashlib.sha224(json.dumps(event1, sort_keys=True))
                           .hexdigest())
      else:
        stream1 = push_back(event1, stream1)

    if event2:
      assert event2[TIMESTAMP_FIELD] >= current_timestamp
      if event2[TIMESTAMP_FIELD] == current_timestamp:
        stream2_hashes.add(hashlib.sha224(json.dumps(event2, sort_keys=True))
                           .hexdigest())
      else:
        stream2 = push_back(event2, stream2)

  print 'Stream 1 Total:', total_stream1
  print 'Stream 2 Total:', total_stream2
  print 'Stream 1 Extra:', extra_stream1
  print 'Stream 2 Extra:', extra_stream2

def process_args():
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--kronos-url1',
    required=True,
    help='The first Kronos server to retrieve data from')
  parser.add_argument(
    '--kronos-url2',
    required=True,
    help='The second Kronos server to retrieve data from')
  parser.add_argument(
    '--namespace1',
    help='The namespace to read from the first Kronos server (optional)')
  parser.add_argument(
    '--namespace2',
    help='The namespace to read from the second Kronos server (optional)')
  parser.add_argument(
    '--stream1',
    required=True,
    help='The stream to read from the first Kronos server')
  parser.add_argument(
    '--stream2',
    required=True,
    help='The stream to read from the second Kronos server')
  parser.add_argument(
    '--start',
    required=True,
    help='When to start retreiving? (format: 2003-09-25T10:49:41.5-03:00)')
  parser.add_argument(
    '--end',
    required=True,
    help='When to end retreiving? (format: 2003-09-25T10:49:41.5-03:00)')
  args = parser.parse_args()
  return args


if __name__ == '__main__':
  main(process_args())
