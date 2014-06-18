#!/usr/bin/python

import os
import unittest

import gevent.monkey; gevent.monkey.patch_all()


if __name__ == '__main__':
  test_suites = unittest.defaultTestLoader.discover(
    start_dir=os.path.join(os.path.dirname(__file__), 'tests'),
    pattern='test_*.py')
  runner = unittest.TextTestRunner(verbosity=2)
  for test_suite in test_suites:
    runner.run(test_suite)
