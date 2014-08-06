#!/usr/bin/python

import os
import unittest

from pykronos.common.runner import KronosRunner

KRONOS_DIR = os.path.join(os.pardir, 'kronos')
KRONOS_CONF = os.path.join(os.pardir,
                           'pykronos/tests/conf/kronos_settings.py')

if __name__ == '__main__':
  kronos_runner = KronosRunner(KRONOS_DIR, config=KRONOS_CONF)
  kronos_runner.start()

  test_suites = unittest.defaultTestLoader.discover(
    start_dir=os.path.join(os.path.dirname(__file__), 'tests'),
    pattern='test_*.py')
  runner = unittest.TextTestRunner(verbosity=2)
  for test_suite in test_suites:
    runner.run(test_suite)

  kronos_runner.stop()
