#!/usr/bin/env python

import os
import shutil

from setuptools import find_packages
from setuptools import setup
from setuptools.command.install import install

import kronos

README = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()
REQUIREMENTS = [
  line.strip() for line in open(os.path.join(os.path.dirname(__file__),
                                             'requirements.txt')).readlines()
  if not line.startswith('git+')
  ]
DEPDENDENCY_LINKS = [
  line.strip() for line in open(os.path.join(os.path.dirname(__file__),
                                             'requirements.txt')).readlines()
  if line.startswith('git+')
  ]

# Add dependency_links to requires:
for link in DEPDENDENCY_LINKS:
  module, version = link.split('=')[1].rsplit('-', 1)
  REQUIREMENTS.append('%s==%s' % (module, version))


class KronosInstall(install):
  def run(self):
    shutil.copy('runserver.py', 'run_kronos.py')
    install.run(self)
    os.system('sudo python scripts/install_kronosd.py')
    os.remove('run_kronos.py')


setup(name='kronos',
      version=kronos.__version__,
      packages=find_packages(exclude=['benchmarks*',
                                      'pykronos*',
                                      'tests*']),
      include_package_data=True,
      license='MIT License',
      description='The Kronos time series storage engine',
      long_description=README,
      url='https://github.com/Locu/chronology/kronos',
      keywords=['kronos', 'analytics', 'metrics', 'client', 'logging'],
      install_requires=REQUIREMENTS,
      dependency_links=DEPDENDENCY_LINKS,
      author='GoDaddy',
      author_email='devs@locu.com',
      cmdclass={
        'install': KronosInstall
        },
      classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        ],
      scripts=[
        'run_kronos.py'
        ]
      )
