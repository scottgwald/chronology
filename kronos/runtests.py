#!/usr/bin/python

import gevent.monkey; gevent.monkey.patch_all()

import functools
import logging
import os
import subprocess
import sys
import unittest

from argparse import ArgumentParser

from kronos.conf import settings
from tests.conf import default_settings
settings.update(default_settings)

# Disable log handlers.
import kronos.app
logging.root.handlers[:] = []
logging.root.addHandler(logging.NullHandler())


# Mapping from test name => (<pathname of directory with test file>,
# <backends to run test against>)
TESTS = {
  'cassandra': ('tests/storage/cassandra', ('cassandra', )),
  'common': ('tests/common', ('memory',
                              'cassandra',
                              'elasticsearch',)),
  'conf': ('tests/conf', ('memory', )),
  'elasticsearch': ('tests/storage/elasticsearch', ('elasticsearch', )),
  }


def use_backend(backend_name):
  from kronos.storage.router import router
  # Use only `backend_name` for both reads and writes.
  for namespace in settings.namespace_to_streams_configuration:
    settings.namespace_to_streams_configuration[namespace] = {
      '': {
        'backends': {
          backend_name: None
          },
        'read_backend': backend_name
        }
      }
  router.reload()


def clear_backend(backend_name):
  from kronos.storage.router import router
  router.get_backend(backend_name)._clear()


def test_against(*backends):
  def decorator(test_name, function):
    @functools.wraps(function)
    def wrapper():
      if 'KRONOS_BACKEND' not in os.environ:
        # Run test for each backend.
        for backend in backends:
          args = [sys.executable, sys.argv[0], test_name]
          new_env = os.environ.copy()
          new_env['KRONOS_BACKEND'] = backend
          # Need this so that for each backend we spawn off a
          # completely new Kronos server. We shouldn't persist state when
          # testing different backends.
          subprocess.call(args, env=new_env)
      else:
        backend = os.environ['KRONOS_BACKEND']
        print '> [%s]: %s' % (backend, test_name)
        use_backend(backend)
        # Run the wrapped test function.
        function()
        # Clear the backend just used.
        clear_backend(backend)
    # Do this so we have a reference to the undecorated function.
    wrapper.wrapped_function = function
    return wrapper
  return decorator


def run_tests_in_dir(path):
  test_suites = unittest.defaultTestLoader.discover(
    start_dir=os.path.join(os.path.dirname(__file__), path),
    pattern='test_*.py')
  runner = unittest.TextTestRunner(verbosity=2)
  for test_suite in test_suites:
    runner.run(test_suite)


def run_test(test_name):
  dir, backends = TESTS[test_name]
  test_against(*backends)(test_name, lambda: run_tests_in_dir(dir))()


if __name__ == '__main__':
  parser = ArgumentParser(description='Kronos test runner.')
  parser.add_argument('tests', nargs='+', help='tests to run')
  args = parser.parse_args()

  if 'all' in args.tests:
    # Run all tests.
    run_test('common')
    run_test('cassandra')
    run_test('conf')
    run_test('elasticsearch')
  else:
    for test in args.tests:
      run_test(test)
