#!/usr/bin/python

import os
import subprocess
import sys
import unittest

from argparse import ArgumentParser


def _configure_kronos(config_name='default'):
  from tests.conf import settings
  settings.configure(config_name)


def run_common_tests():
  # Need this so that for each configuration we spawn off a completely
  # new Kronos server. We shouldn't persist state when testing different
  # configurations.
  if 'KRONOS_CONFIG' not in os.environ:
    # Run common tests with all configurations.
    for config in ('memory', ):#'cassandra'):
      args = [sys.executable]
      args.extend(sys.argv)
      if '--common' not in args:
        args.append('--common')
      new_env = os.environ.copy()
      new_env['KRONOS_CONFIG'] = config
      subprocess.call(args, env=new_env)
  else:
    # Configure Kronos with the right settings before running the tests.
    _configure_kronos(os.environ['KRONOS_CONFIG'])
    test_suites = unittest.defaultTestLoader.discover(
      start_dir=os.path.join(os.path.dirname(__file__), 'tests/common'),
      pattern='test_*.py')
    runner = unittest.TextTestRunner()
    for test_suite in test_suites:
      runner.run(test_suite)

if __name__ == '__main__':
  parser = ArgumentParser(description='Kronos test runner.')
  parser.add_argument('--common', action='store_true',
                      help='run common tests only?')
  parser.add_argument('--cassandra', action='store_true',
                      help='run Cassandra tests only?')
  args = parser.parse_args()

  if args.common:
    run_common_tests()
  else:
    # Run all tests.
    run_common_tests()
