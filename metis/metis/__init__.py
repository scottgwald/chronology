import os
import sys
import zipfile

from flask import Flask

_METIS_PATH = os.path.realpath(os.path.join(os.path.dirname(__file__),
                                            os.pardir))

app = Flask(__name__)
app.config.from_pyfile('%s/settings.cfg' % _METIS_PATH)

VERSION = (0, 1, 'alpha')

def get_version(version=None):
  version = version or VERSION
  assert(len(version) == 3)
  return '%s.%s %s' % version

def _setup_pyspark():
  # Set SPARK_HOME environment variable.
  os.putenv('SPARK_HOME', app.config['SPARK_HOME'])
  # From Python docs: Calling putenv() directly does not change os.environ, so 
  # it's better to modify os.environ. Also some platforms don't support
  # os.putenv. We'll just perform both.
  os.environ['SPARK_HOME'] = app.config['SPARK_HOME']
  # Add PySpark to path.
  sys.path.append(os.path.join(app.config['SPARK_HOME'], 'python'))

_setup_pyspark()

def _create_archive_for_pyspark():
  zip_file_path = '%s/metis.zip' % _METIS_PATH
  if not app.debug and zipfile.is_zipfile(zip_file_path):
    # If zip file is present and we're not running in debug mode, don't
    # regenerate it.
    return
  walk_path = '%s/metis/' % _METIS_PATH
  zip_file = zipfile.ZipFile(zip_file_path, 'w')
  # TODO(usmanm): Zip only the minimum set of files needed.
  for root, dirs, files in os.walk(walk_path):
    for _file in files:
      if root == walk_path and _file.startswith('__init__.py'):
        # Don't copy this file. Instead we create an empty file as a
        # replacement.
        continue
      zip_file.write(os.path.join(root, _file),
                     os.path.join(root.replace(_METIS_PATH, ''), _file))
  # TODO(usmanm): Hack to ensure __init__.py file is empty and has no
  # initialization code.
  zip_file.write(os.path.join(_METIS_PATH, 'metis/__main__.py'),
                 'metis/__init__.py')
  zip_file.close()
  app.config['METIS_ZIP_FILE'] = zip_file_path

_create_archive_for_pyspark()

import metis.views
