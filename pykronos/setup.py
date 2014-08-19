#!/usr/bin/env python

import os

from setuptools import find_packages
from setuptools import setup

exec(open('pykronos/version.py').read())

README = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()
REQUIREMENTS = [
  line.strip() for line in open(os.path.join(os.path.dirname(__file__),
                                             'requirements.txt')).readlines()
  ]

setup(name='pykronos',
      version=__version__,
      packages=find_packages(exclude=['tests*']),
      include_package_data=True,
      license='MIT License',
      description='A Python client for the Kronos time series storage engine',
      long_description=README,
      url='https://github.com/Locu/chronology/pykronos',
      keywords=['kronos', 'analytics', 'metrics', 'client', 'logging'],
      install_requires=REQUIREMENTS,
      author='GoDaddy',
      author_email='devs@locu.com',
      classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
          ],
      )
