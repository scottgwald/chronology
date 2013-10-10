#!/usr/bin/python

import functools
import os
import subprocess
import sys
import unittest

from argparse import ArgumentParser

def test_against(*configs):
  def decorator(function):
    @functools.wraps(function)
    def wrapper():
      if 'KRONOS_CONFIG' not in os.environ:
        # Run test for each configuration.
        for config in configs:
          name = function.func_name.lstrip('test_')
          args = [sys.executable, sys.argv[0], name]
          new_env = os.environ.copy()
          new_env['KRONOS_CONFIG'] = config
          # Need this so that for each configuration we spawn off a completely
          # new Kronos server. We shouldn't persist state when testing different
          # configurations.
          subprocess.call(args, env=new_env)
      else:
        # Configure Kronos with the right settings before running the tests.
        from tests.conf import settings
        config = os.environ['KRONOS_CONFIG']
        settings.configure(config)
        # Run the wrapped test function.
        function()
        # Do any teardown needed for some Kronos configurations.
        if config == 'cassandra':
          from kronos.storage import router
          for namespace in (router.get_backend('cassandra').namespaces
                            .itervalues()):
            namespace._drop()
    wrapper.wrapped_function = function
    return wrapper
  return decorator


@test_against('memory', 'cassandra')
def test_common():
  test_suites = unittest.defaultTestLoader.discover(
    start_dir=os.path.join(os.path.dirname(__file__), 'tests/common'),
    pattern='test_*.py')
  runner = unittest.TextTestRunner()
  for test_suite in test_suites:
    runner.run(test_suite)


@test_against('cassandra')
def test_cassandra():
  # Run all the common tests on Cassandra.
  test_common.wrapped_function()


def run_test(test_name):
  test_function = getattr(sys.modules[__name__], 'test_%s' % test_name)
  test_function()


if __name__ == '__main__':
  parser = ArgumentParser(description='Kronos test runner.')
  parser.add_argument('tests', nargs='+', help='tests to run')
  args = parser.parse_args()

  if 'against' in args.tests:
    raise ValueError

  if 'all' in args.tests:
    # Run all tests
    run_test('common')
  else:
    for test in args.tests:
      run_test(test)
