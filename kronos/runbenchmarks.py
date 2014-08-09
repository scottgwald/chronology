#!/usr/bin/python

import gevent.monkey; gevent.monkey.patch_all()

import os
import time
import traceback

from argparse import ArgumentParser
from glob import glob

from kronos.common.runner import KronosRunner

KRONOS_DIR = os.path.realpath(os.path.dirname(__file__))


def run_benchmark(benchmark):
  start = time.time()
  print '> [%s]: running benchmark...' % benchmark
  kronos_runner = KronosRunner(KRONOS_DIR,
                               config='benchmarks/%s/settings.py' % benchmark)
  kronos_runner.start()
  
  for filename in glob('benchmarks/%s/*.py' % benchmark):
    module_name = filename.split('/')[-1].split('.')[0]
    if module_name in ('__init__', 'settings'):
      continue

    print '> [%s]: %s' % (benchmark, module_name)
    try:
      exec 'from benchmarks.%s.%s import run; run()' % (benchmark, module_name)
    except Exception, e:
      traceback.print_exc(e)
      
  print '> [%s]: cleaning up...' % benchmark
  try:
    exec 'from benchmarks.%s import clean; clean()' % benchmark
  except Exception, e:
    traceback.print_exc(e)
  kronos_runner.stop()
  print '> [%s]: took %ss' % (benchmark, time.time() - start)
  print

if __name__ == '__main__':
  parser = ArgumentParser(description='Kronos benchmark runner.')
  parser.add_argument('benchmarks', nargs='+', help='benchmarks to run')
  args = parser.parse_args()

  if 'all' in args.benchmarks:
    # Run all benchmarks.
    args.benchmarks = ['cassandra', 'elasticsearch']
  for benchmark in args.benchmarks:
    run_benchmark(benchmark)
