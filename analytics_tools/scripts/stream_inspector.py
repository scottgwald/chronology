#!/usr/bin/python
# coding: utf-8
"""
A script to inspect a Kronos stream.

example usage:
  python scripts/stream_inspector.py --start 2014-01-7T10:49:41.5-03:00 --end 2014-09-25T11:49:41.5-02:00 --kronos-url http://your-kronos.instance:port --stream the.name.of.a.stream print
  python scripts/stream_inspector.py --start 2014-01-7T10:49:41.5-03:00 --end 2014-09-25T11:49:41.5-02:00 --kronos-url http://your-kronos.instance.com:port --stream the.name.of.a.stream csv --fields field1 field2
  python scripts/stream_inspector.py --start 2014-01-7T10:49:41.5-03:00 --end 2014-09-25T11:49:41.5-02:00 --kronos-url http://your-kronos.instance.com:port --stream the.name.of.a.stream aggregate --field field1 --aggregator count --time-bucket-width [3600 would be an hour]
"""

import argparse
import csv
import json
import logging
import sys

from analytics_tools.aggregates import AGGREGATORS
from analytics_tools.aggregates import aggregate_stream
from datetime import datetime
from dateutil.parser import parse
from common.event_tools import get_property
from pykronos import KronosClient

log = logging.getLogger(__name__)

def main(args):
  client = KronosClient(args.kronos_url)
  results = client.get(args.stream, args.start, args.end,
                       namespace=args.namespace)
  if args.display == 'print':
    if args.type == 'json':
      events = []
      for event in results:
        events.append(event)
      print json.dumps(events)
    elif args.type == 'one-per-line':
      for event in results:
        print event
  elif args.display == 'csv':
    writer = csv.DictWriter(sys.stdout, args.fields)
    if not args.remove_header:
      writer.writeheader()
    for event in results:
      row_values = {}
      for field in args.fields:
        field_value = get_property(event, field)
        row_values[field] = (field_value.encode('utf-8')
                             if isinstance(field_value, unicode)
                             else field_value)
      writer.writerow(row_values)
  elif args.display == 'aggregate':
    aggregates = aggregate_stream(results, AGGREGATORS[args.aggregator],
                                  args.field, args.time_bucket_width)
    print 'Bucket, Aggregate'
    for bucket, aggregate in aggregates:
      print '%s, %s' % (datetime.fromtimestamp(bucket), aggregate)
  else:
    raise Exception('Invalid display option {}'.format(args.display))


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
  parser.add_argument('--stream',
                      required=True,
                      help='The name of the stream to analyze')
  parser.add_argument('--kronos-url',
                      required=True,
                      help='The Kronos server to retrieve data from')
  parser.add_argument('--namespace',
                      help='The namespace to read from (optional)')
  subparsers = parser.add_subparsers(help='How to display the results',
                                     dest='display')
  print_parser = subparsers.add_parser(
      'print',
      help='Print the events, one per line, or JSON array')
  print_parser.add_argument('--type',
                            choices=('json', 'one-per-line'),
                            default='one-per-line')
  field_parser = subparsers.add_parser('csv',
                                       help=('Project out fields and print '
                                             'them in csv format. Reference '
                                             'nested fields with \'.\''))
  field_parser.add_argument('--fields',
                            required=True,
                            nargs='+',
                            help='The fields to project and print in csv format')
  field_parser.add_argument('--remove-header',
                            action='store_true',
                            help="Don't print the header?")

  aggregate_parser = subparsers.add_parser('aggregate',
                                           help=('Show aggregate results'))
  aggregate_parser.add_argument('--field',
                                required=True,
                                help='The field to run the aggregate on')
  aggregate_parser.add_argument('--aggregator',
                                required=True,
                                choices=AGGREGATORS.keys(),
                                help='The aggregate function to run')
  aggregate_parser.add_argument('--time-bucket-width',
                                type=int,
                                required=True,
                                help='The number of seconds to bucket events into')
                                           

  args = parser.parse_args()
  args.start = parse(args.start)
  args.end = parse(args.end)
  return args


if __name__ == '__main__':
  main(process_args())
