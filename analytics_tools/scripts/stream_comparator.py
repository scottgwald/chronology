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
import random
import sys

from dateutil.parser import parse
from pykronos import KronosClient
from pykronos import ID_FIELD
from pykronos import TIMESTAMP_FIELD
from pykronos.common.time import epoch_time_to_kronos_time
from pykronos.common.time import datetime_to_kronos_time

log = logging.getLogger(__name__)


def get_next(events):
  try:
    return events.next()
  except StopIteration:
    return None


def push_back(event, iterator):
  return itertools.chain([event], iterator)


def main(args):
  client1 = KronosClient(args.kronos_url1, namespace=args.namespace1)
  client2 = KronosClient(args.kronos_url2, namespace=args.namespace2)

  if args.streams_file:
    streams = map(lambda s: (s, s), # Use same stream name for both.
                  filter(lambda s: len(s),
                         open(args.streams_file).read().split('\n')))
  else:
    streams = [(args.stream1, args.stream2)]

  for stream1_name, stream2_name in streams:
    if args.num_samples:
      samples = []
      for _ in xrange(args.num_samples):
        start = random.randint(args.start, args.end - args.sample_interval)
        samples.append((start, start + args.sample_interval))
    else:
      samples = [(args.start, args.end)]

    total_stream1 = 0
    extra_stream1 = 0
    total_stream2 = 0
    extra_stream2 = 0

    for start, end in samples:
      stream1 = client1.get(stream1_name, start, end)
      stream2 = client2.get(stream2_name, start, end)

      # Sorting of events with the same timestamp may vary across backends,
      # hence we can't do a simple loop comparison. We need to aggregate all
      # events with the same timestamp from both streams and then compare the
      # two sets.
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
        # If min_timestamp is greater than current_timestamp, then aggregate
        # stats for current_timestamp and roll over.
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
            del event1[ID_FIELD]
            stream1_hashes.add(
              hashlib.sha224(json.dumps(event1, sort_keys=True)).hexdigest())
          else:
            stream1 = push_back(event1, stream1)

        if event2:
          assert event2[TIMESTAMP_FIELD] >= current_timestamp
          if event2[TIMESTAMP_FIELD] == current_timestamp:
            del event2[ID_FIELD]
            stream2_hashes.add(
              hashlib.sha224(json.dumps(event2, sort_keys=True)).hexdigest())
          else:
           stream2 = push_back(event2, stream2)

    print 'Diff: [%s/%s], [%s/%s]' % (args.namespace1, stream1_name,
                                      args.namespace2, stream2_name)
    print '< total: %d' % total_stream1
    print '> total: %d' % total_stream2
    print '< extra: %d' % extra_stream1
    print '> extra: %d' % extra_stream2
    print 

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
    help='The stream to read from the first Kronos server')
  parser.add_argument(
    '--stream2',
    help='The stream to read from the second Kronos server')
  parser.add_argument(
    '--streams-file',
    help='The name of the file with a stream name per line to copy')
  parser.add_argument(
    '--start',
    required=True,
    help='When to start retreiving? (format: 2003-09-25T10:49:41.5-03:00)')
  parser.add_argument(
    '--end',
    required=True,
    help='When to end retreiving? (format: 2003-09-25T10:49:41.5-03:00)')
  group = parser.add_argument_group(title='sampling arguments',
                                    description=('Only compare samples of '
                                                 'events.'))
  group.add_argument('--num-samples',
                     type=int,
                     help=('Number of samples to compare?'))
  group.add_argument('--sample-interval',
                     type=int,
                     default=3600,
                     help=('Interval of the sample (in seconds). Defaults to '
                           '1 hour (3600) (optional)'))
  args = parser.parse_args()

  if not bool(args.streams_file) ^ bool(args.stream1 and args.stream2):
    print 'Please specify either `stream-file` or both `stream1 and stream2`.'
    sys.exit(1)

  args.start = datetime_to_kronos_time(parse(args.start))
  args.end = datetime_to_kronos_time(parse(args.end))
  args.sample_interval = epoch_time_to_kronos_time(args.sample_interval)
  
  return args


if __name__ == '__main__':
  main(process_args())
