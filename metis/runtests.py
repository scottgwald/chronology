#!/usr/bin/python

import os
import unittest

from metis import app
from metis.common.runner import (KronosRunner,
                                 MetisRunner)

KRONOS_DIR = os.path.join(os.pardir, 'kronos')
KRONOS_CONF = os.path.join(os.pardir,
                           'metis/tests/conf/kronos_settings.py')
METIS_DIR = os.path.realpath(os.path.dirname(__file__))
# Note: you might have to change SPARK_HOME in settings.py below to
# match your spark home.
METIS_CONF = 'tests/conf/settings.py'

if __name__ == '__main__':
  kronos_runner = KronosRunner(KRONOS_DIR, config=KRONOS_CONF)
  kronos_runner.start()
  metis_runner = MetisRunner(METIS_DIR, config=METIS_CONF)
  metis_runner.start()

  app.config.from_pyfile(os.path.join(os.pardir, METIS_CONF))

  test_suites = unittest.defaultTestLoader.discover(
    start_dir=os.path.join(os.path.dirname(__file__), 'tests'),
    pattern='test_*.py')
  runner = unittest.TextTestRunner(verbosity=2)
  for test_suite in test_suites:
    runner.run(test_suite)

  kronos_runner.stop()
  metis_runner.stop()
