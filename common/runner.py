import os
import subprocess
import time

from multiprocessing import Pipe
from multiprocessing import Process


class ProcessRunnerError(Exception):
  pass


class ProcessRunner(object):
  def __init__(self, args, cwd=None, wait=None):
    '''
    Runs a shell command in a separate process.
    `args` - list of args that represent the shell command.
    `cwd` - working directory to execute the shell command from.
    '''
    self.args = args
    if cwd and not os.path.isabs(cwd):
      cwd = os.path.join(os.getcwd(), cwd)
    self.cwd = cwd
    self.wait = wait
    # `self._proc` is the multiprocessing.Process that executes the shell
    # command.
    # `self._sub_proc` is the subprocess.Popen `self._proc` runs for executing
    # the shell command.
    # `self._proc_pipe` is a duplex pipe connecting the parent process and
    # `self._proc`.
    self._proc = self._sub_proc = self._proc_pipe = None

  def _subprocess_target(self, pipe):
    os.chdir(self.cwd)  
    sub_proc = subprocess.Popen(self.args,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    if self.wait:
      time.sleep(self.wait)
    pipe.send(sub_proc)

  def start(self):
    if self._proc:
      raise ProcessRunnerError('Process already running!')
    self._proc_pipe, child_conn = Pipe()
    self._proc = Process(target=self._subprocess_target, args=(child_conn,))
    self._proc.start()
    self._sub_proc = self._proc_pipe.recv()
  
  def stop(self):
    if not self._proc:
      raise ProcessRunnerError('Process not running!')
    self._sub_proc.kill()
    self._proc.terminate()

  def restart(self):
    if self._proc:
      self.stop()
    self.start()


class KronosRunner(ProcessRunner):
  def __init__(self, kronos_dir, config_dir=None, port='9191', wait=2):
    super(KronosRunner, self).__init__(
      ['python', 'kronos.py',
       '--config', config_dir,
       '--bind', '0.0.0.0:%s' % port,
       '--debug'],
      cwd=kronos_dir,
      wait=wait)
