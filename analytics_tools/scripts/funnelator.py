#!/usr/bin/python
# coding: utf-8
"""
A script to calculate funnels from various kronos streams that all
share the same user-identifying field.

example usage:
  python scripts/funnelator.py --kronos-url http://kronos-url.com:port --user-field userId --start 2013-09-25T10:49:41.5-03:00 --end 2014-09-25T10:49:41.5-03:00 --streams stream1 stream2 stream3
"""
import argparse
import logging

from analytics_tools.funnels import funnel_analyze

from dateutil.parser import parse
from pykronos import KronosClient

log = logging.getLogger(__name__)

def main(args):
  client = KronosClient(args.kronos_url, namespace=args.namespace)
  unfiltered_streams = [(stream, None, args.user_field)
                        for stream in args.streams]
  stream_sizes = funnel_analyze(client, unfiltered_streams, args.start,
                                args.end, args.end, {}, None)
  # TODO(marcua): print something more meaningful here.
  print stream_sizes
  
  
def process_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--start',
                      required=True,
                      help=('When to start retrieving? (format: '
                            '2003-09-25T10:49:41.5-03:00)'))
  parser.add_argument('--end',
                      required=True,
                      help=('When to end retrieving? (format: '
                            '2003-09-25T10:49:41.5-03:00)'))  
  parser.add_argument('--debug',
                      action='store_true', default=False,
                      help='Display debug messages')
  parser.add_argument('--streams',
                      required=True,
                      nargs='+',
                      help=('The streams to run the funnel on, in the '
                            'order of the funnel'))
  parser.add_argument('--kronos-url',
                      required=True,
                      help='The Kronos server to retrieve data from')
  parser.add_argument('--user-field',
                      required=True,
                      help='The field on each event that identifies the user')
  parser.add_argument('--namespace',
                      help='The namespace to read from (optional)')

  args = parser.parse_args()
  args.start = parse(args.start)
  args.end = parse(args.end)
  if args.debug:
    logging.basicConfig(level=logging.DEBUG)
  return args


if __name__ == '__main__':
  main(process_args())
