import imp
import os

from kronos.common.settings import Settings
from kronos.conf.constants import SETTINGS_PATH

settings = Settings()

if os.path.isfile(SETTINGS_PATH) and os.access(SETTINGS_PATH, os.R_OK):
  # This is where the kronos service expects the settings.py file to live.
  settings.update(imp.load_source('kronos.conf.tmp_settings', SETTINGS_PATH))
else:
  from kronos.conf import default_settings
  settings.update(default_settings)
