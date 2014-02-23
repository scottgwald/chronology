#!/usr/bin/python

import os
import shutil
import subprocess
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        os.path.pardir))

def create_user_and_group():
  sys.stdout.write('Creating kronos user and group accounts... ')
  with open(os.devnull, 'wb') as devnull:
    subprocess.call(['sudo', 'useradd', 'kronos'],
                    stdout=devnull, stderr=devnull)
    subprocess.call(['sudo', 'groupadd', 'kronos'],
                    stdout=devnull, stderr=devnull)
  print 'done.'

def make_dirs():
  def safe_mkdir(path):
    try:
      os.makedirs(path)
    except OSError as e:
      if e.errno != 17:
        raise e
  
  sys.stdout.write('Creating directories... ')
  safe_mkdir('/etc/kronos')
  safe_mkdir('/var/log/kronos')
  safe_mkdir('/var/run/kronos')
  print 'done.'

def copy_files():
  sys.stdout.write('Copying configuration and init.d script files... ')
  shutil.copy(os.path.join(BASE_DIR, 'scripts/uwsgi.ini'),
              '/etc/kronos/uwsgi.ini')
  shutil.copy(os.path.join(BASE_DIR, 'scripts/kronosd.init.d'),
              '/etc/init.d/kronos')
  shutil.copy(os.path.join(BASE_DIR, 'settings.py.template'),
              '/etc/kronos/settings.py')
  print 'done.'

def install_kronosd():
  create_user_and_group()
  make_dirs()
  copy_files()

if __name__ == '__main__':
  install_kronosd()
