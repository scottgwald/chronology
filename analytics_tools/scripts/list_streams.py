#!/usr/bin/python
# coding: utf-8
"""
A script to list the available streams on a Kronos server.

example usage:
  python scripts/list_streams.py --kronos-url http://your-kronos.instance:port [--namespace some_namespace]

  python scripts/list_streams.py --kronos-url http://your-kronos.instance:port [--namespace some_namespace] --start 2014-05-25 --end 2014-05-26 --fetch-timeout 60

  python scripts/list_streams.py --kronos-url http://your-kronos.instance:port [--namespace some_namespace] --read-latency 0.5
"""

import gevent.monkey; gevent.monkey.patch_all()

import argparse
import logging
import os
import sys

from datetime import datetime
from gevent import Greenlet
from pykronos import KronosClient

log = logging.getLogger(__name__)


def check_stream(client, namespace, stream, start, end, limit, timeout,
                 latency):
  def run():
    for event in client.get(stream, start, end, limit=limit, timeout=latency):
      # Yeah, I'm useless.
      pass

  read_greenlet = Greenlet(run)
  read_greenlet.start()
  read_greenlet.join(timeout)
  if not read_greenlet.ready():
    read_greenlet.kill()
    success = False
  else:
    success = read_greenlet.successful()
  return success

def main(args):
  client = KronosClient(args.kronos_url)
  if args.fetch_timeout:
    start = args.start
    end = args.end
    limit = None
  else:
    start = 0
    end = datetime.utcnow()
    limit = 1000

  for stream in client.get_streams(namespace=args.namespace):
    if not (args.read_latency or args.fetch_timeout):
      print stream
    elif check_stream(client, args.namespace, stream, start, end, limit,
                      args.fetch_timeout, args.read_latency):
      print stream

def process_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--kronos-url',
                      required=True,
                      help='The Kronos server to retrieve data from')
  parser.add_argument('--namespace',
                      help='The namespace to read from (optional)')
  parser.add_argument('--read-latency',
                      type=float,
                      help=('Only return streams where read latency is at most '
                            'this value. Specify in seconds. (optional).'))
  group = parser.add_argument_group(title='fetch latency arguments',
                                    description=('Either all of none of these '
                                                 'args should be present.'))
  group.add_argument('--start',
                     help=('When to start retreiving? (format: '
                           '2003-09-25T10:49:41.5-03:00)'))
  group.add_argument('--end',
                     help=('When to end retreiving? (format: '
                           '2003-09-25T10:49:41.5-03:00)'))
  group.add_argument('--fetch-timeout',
                     type=float,
                     help=('Total time to wait for all events to be fetched '
                           'between start and end. Only return streams where '
                           'all events are fetched within timout (in seconds)'))
  args = parser.parse_args()

  if (any((args.start, args.end, args.fetch_timeout)) and
      not all((args.start, args.end, args.fetch_timeout))):
    print ('Either all or none of `start`, `end` and `fetch-timeout` should '
           'be present.')
    sys.exit(1)

  return args


if __name__ == '__main__':
  args = process_args()

  # TODO(usmanm): Greenlets spit out exception raised within them to stderr,
  # which pollutes the output in a shell. Redirect stderr to dev/null.
  devnull = open(os.devnull, 'w')
  sys.stderr = devnull
  
  main(args)
