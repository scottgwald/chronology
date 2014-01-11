#!/usr/bin/python

import os
import signal
import subprocess
import time
import unittest

KRONOS_DIR = os.path.join(os.pardir, 'kronos')
KRONOS_CONF = os.path.join(os.pardir, 'pykronos/tests/conf/kronos_settings.py')

if __name__ == '__main__':
  # TODO(usmanm): Fix this to run Kronos service.
  cwd = os.getcwd()
  os.chdir(os.path.join(cwd, KRONOS_DIR))
  FNULL = open(os.devnull, 'w')
  kronos_proc = subprocess.Popen(['python', 'kronos.py',
                                  '--config', KRONOS_CONF,
                                  '--bind', '0.0.0.0:9191',
                                  '--num-proc', '1',
                                  '--print-pid'],
                                 stdout=subprocess.PIPE,
                                 stderr=FNULL)
  # Wait for Kronos to fire up and get its PID.
  while True:
    kronos_pid = kronos_proc.stdout.readline().strip()
    if not kronos_pid:
      time.sleep(0.1)
    time.sleep(0.5)
    break

  os.chdir(cwd)

  test_suites = unittest.defaultTestLoader.discover(
    start_dir=os.path.join(os.path.dirname(__file__), 'tests'),
    pattern='test_*.py')
  runner = unittest.TextTestRunner()
  for test_suite in test_suites:
    runner.run(test_suite)

  # Kill the Kronos process.
  os.kill(int(kronos_pid), signal.SIGTERM)
