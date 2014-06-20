#!/usr/bin/python
# coding: utf-8
"""
A script to bulk-load a bunch of data into a Kronos server for testing
purposes.

example usage:
  python scripts/stream_bulkloader.py --kronos-url http://your-kronos.instance:port  --namespace namespace --start 2014-06-01T00:00:00  --stream stream_name --num-events 10000 --microseconds-between-events 5000 --properties-per-event 3
"""

import argparse
import logging
import time
from datetime import timedelta
from dateutil.parser import parse
from pykronos import KronosClient
from pykronos import TIMESTAMP_FIELD

log = logging.getLogger(__name__)

def main(args):
  client = KronosClient(args.kronos_url, namespace=args.namespace,
                        blocking=False)
  increment = timedelta(microseconds=args.microseconds_between_events)
  event = {'property%s' % (idx): idx 
           for idx in xrange(args.properties_per_event)}
  start_time = time.time()
  for idx in xrange(args.num_events):
    event[TIMESTAMP_FIELD] = args.start + (idx * increment)
    client.put({args.stream: [event]})
    if (idx % args.chunk_size) == 0:
      print 'Completed', idx, 'events', time.time() - start_time
      client.flush()
  client.flush()


def process_args():
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--kronos-url',
    required=True,
    help='The Kronos server to write to')
  parser.add_argument(
    '--namespace',
    help='The namespace to write to')
  parser.add_argument(
    '--stream',
    required=True,
    help='The name of the stream to write to')
  parser.add_argument(
    '--start',
    required=True,
    help='Start date of events (format: 2003-09-25T10:49:41.5-03:00)')
  parser.add_argument(
    '--num-events',
    type=int,
    required=True,
    help='The number of events to write')
  parser.add_argument(
    '--microseconds-between-events',
    type=int,
    required=True,
    help='The microseconds between events')
  parser.add_argument(
    '--properties-per-event',
    type=int,
    required=True,
    help='The number of properties to put on each event (controls its size)')
  parser.add_argument(
    '--chunk-size',
    type=int,
    default=10000,
    help='Flush after writing this many events')
  args = parser.parse_args()
  args.start = parse(args.start)
  return args


if __name__ == '__main__':
  main(process_args())
