from __future__ import absolute_import

import os
import subprocess
import sys
import time
import urllib2


def _http_probe_wait(url, sleep_time=0.05, num_probes=50):
  for i in xrange(num_probes):
    try:
      urllib2.urlopen(url)
      return True
    except urllib2.URLError:
      time.sleep(sleep_time)
  return False


class ProcessRunnerError(Exception):
  pass


class ProcessRunner(object):
  def __init__(self, args, cwd=None, verbose=False):
    '''
    Runs a shell command in a separate process.
    `args` - list of args that represent the shell command.
    `cwd` - working directory to execute the shell command from.
    '''
    self.args = args
    if cwd and not os.path.isabs(cwd):
      cwd = os.path.join(os.getcwd(), cwd)
    self.cwd = cwd
    self.verbose = verbose
    self._sub_proc = None

  def start(self):
    if self._sub_proc:
      raise ProcessRunnerError('Process already running!')
    if self.verbose:
      stdout = sys.stdout
      stderr = sys.stderr
    else:
      stdout = stderr = open(os.devnull, 'w')
    self._sub_proc = subprocess.Popen(self.args,
                                      stdout=stdout,
                                      stderr=stderr,
                                      cwd=self.cwd)
    self.wait()

  def wait(self):
    pass

  def stop(self):
    if not self._sub_proc:
      raise ProcessRunnerError('Process not running!')
    self._sub_proc.terminate()
    self._sub_proc.wait()

  def restart(self):
    if self._sub_proc:
      self.stop()
    self.start()


class KronosRunner(ProcessRunner):
  def __init__(self, kronos_dir, config=None, port='9191', **kwargs):
    self.port = port
    args = ['python', 'runserver.py',
            '--port', port,
            '--debug']
    if config:
      args.extend(['--config', config])

    super(KronosRunner, self).__init__(args, cwd=kronos_dir, **kwargs)

  def wait(self):
    if not _http_probe_wait('http://localhost:%s/1.0/index' % self.port):
      raise ProcessRunnerError('Failed to start KronosRunner.')


class MetisRunner(ProcessRunner):
  def __init__(self, metis_dir, config=None, port='9192', **kwargs):
    self.port = port
    args = ['python', 'runserver.py',
            '--port', port,
            '--debug']
    if config:
      args.extend(['--config', config])

    super(MetisRunner, self).__init__(args, cwd=metis_dir, **kwargs)

  def wait(self):
    if not _http_probe_wait('http://localhost:%s/1.0/index' % self.port):
      raise ProcessRunnerError('Failed to start MetisRunner.')
