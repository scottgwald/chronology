import types

from kronos.storage.backends import BaseStorage


class S3Storage(BaseStorage):
  SETTINGS_VALIDATORS = {
    'aws_access_key_id': lambda x: isinstance(x, types.StringTypes),
    'aws_secret_access_key': lambda x: isinstance(x, types.StringTypes),
    'bucket_name': lambda x: isinstance(x, types.StringTypes),
    'flush_interval': lambda x: int(x) > 0,
    'local_dir': lambda x: isinstance(x, types.StringTypes),
    'time_width': lambda x: int(x) > 0
  }
  
  def __init__(self, name, **settings):
    super(S3Storage, self).__init__(name, **settings)
    self.settings = settings

  def is_alive(self):
    return True

  def _insert(self, namespace, stream, events, configuration):
    pass

  def _delete(self, namespace, stream, start_id, end_time, configuration):
    pass

  def _retrieve(self, namespace, stream, start_id, end_time, order, limit,
                configuration):
    pass

  def _streams(self, namespace):
    pass
