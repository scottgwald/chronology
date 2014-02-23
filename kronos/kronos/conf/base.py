import imp
import os

from kronos.conf import default_settings

class Settings(object):
  def __init__(self, settings_module=None):
    if not settings_module:
      if os.path.isfile('/etc/kronos/settings.py'):
        settings_module = imp.load_source('kronos.conf.run_settings',
                                          '/etc/kronos/settings.py')
      else:
        settings_module = default_settings
    self.settings_module = settings_module

  def use(self, settings_module):
    self.settings_module = settings_module

  def __getattr__(self, attr):
    return getattr(self.settings_module, attr)
