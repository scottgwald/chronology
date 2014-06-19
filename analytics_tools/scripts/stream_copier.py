#!/usr/bin/python
# coding: utf-8
"""
A script to copy certain streams between two different kronos
server/namespace instances.

example usage:
  python scripts/stream_copier.py --read-url http://your-kronos.instance1:port --read-namespace namespace1 --write-url http://your-kronos.instance1:port --write-namespace namespace2 --stream-file filename --start 2014-06-01T00:00:00 --end 2014-06-02T00:00:00
  -> stream-file is a file containing the name of streams to copy, with one stream per line.
"""

import argparse
import logging
import time
from pykronos import KronosClient
from pykronos import ID_FIELD

log = logging.getLogger(__name__)

def main(args):
  read_client = KronosClient(args.read_url, namespace=args.read_namespace)
  write_client = KronosClient(args.write_url, namespace=args.write_namespace,
                              blocking=False)
  start_time = time.time()
  for stream in args.stream_file:
    stream = stream.rstrip()
    print 'Starting stream', stream, time.time() - start_time
    read_stream = read_client.get(stream, args.start, args.end)
    for idx, event in enumerate(read_stream):
      del event[ID_FIELD]
      write_client.put({stream: [event]})
      if (idx % args.chunk_size) == 0:
        print '...', idx, 'events inserted', time.time() - start_time
    write_client.flush()
    print 'Completed stream', stream, time.time() - start_time    


def process_args():
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--read-url',
    required=True,
    help='The Kronos server to retrieve data from')
  parser.add_argument(
    '--write-url',
    required=True,
    help='The Kronos server to write data to')
  parser.add_argument(
    '--read-namespace',
    required=True,
    help='The namespace to read data from')
  parser.add_argument(
    '--write-namespace',
    required=True,
    help='The namespace to write data to')
  parser.add_argument(
    '--stream-file',
    required=True,
    help='The name of the file with a stream name per line to copy')
  parser.add_argument(
    '--start',
    required=True,
    help='When to start retreiving? (format: 2003-09-25T10:49:41.5-03:00)')
  parser.add_argument(
    '--end',
    required=True,
    help='When to end retreiving? (format: 2003-09-25T10:49:41.5-03:00)')
  parser.add_argument(
    '--chunk-size',
    type=int,
    default=10000,
    help='Flush after writing this many events')
  args = parser.parse_args()
  args.stream_file = open(args.stream_file, 'r')
  return args


if __name__ == '__main__':
  main(process_args())
