# Width smaller than 86400 seconds are not recommended since it can have 
# huge implications on the amount of memory used by Redis.

import math
import random
import redis
import types

from collections import defaultdict

from kronos.core.exceptions import ImproperlyConfigured
from kronos.core.validators import TIMESTAMP_FIELD
from kronos.storage.backends import BaseStorage

class EventUserSetRedisStorage(BaseStorage):
  SETBIT_LUA = ('for k, v in ipairs(ARGV) do\n'
                '  redis.call("setbit", KEYS[1], v, 1)\n'
                'end')
  CONF_PARAMETERS = {
    'user_id_field': lambda x: isinstance(x, types.StringTypes),
    'width': int
  }

  def __init__(self, name, **settings):
    super(EventUserSetRedisStorage, self).__init__(name, settings)
    self.connection = redis.StrictRedis(host=settings['host'],
                                        port=settings['port'],
                                        db=settings['db'])
    self.default_width = int(settings['default_width'])
    if not self.default_width > 0:
      raise ImproperlyConfigured('`default_width` must be a positive integer.')
    self.default_user_id_field = settings['default_user_id_field']
    self.key_prefix = settings['key_prefix']

  def is_alive(self):
    return self.connection.ping()

  def floor_mod(self, value, mod):
    return int(math.floor(value / float(mod)) * mod)

  def ceil_mod(self, value, mod):
    return int(math.ceil(value / float(mod)) * mod)

  def insert(self, stream, events, configuration):
    width = configuration.get('width', self.default_width)
    user_id_field = configuration.get('user_id_field', 
                                      self.default_user_id_field)
    bits_to_set_per_interval = defaultdict(set)
    for event in events:
      if user_id_field not in event:
        continue
      interval = self.floor_mod(event[TIMESTAMP_FIELD], width)
      bits_to_set_per_interval[interval].add(event[user_id_field])
    for interval in bits_to_set_per_interval:
      key = '%s:%s:%s' % (self.key_prefix, stream, interval)
      self.connection.eval(EventUserSetRedisStorage.SETBIT_LUA, 1, key,
                           **list(bits_to_set_per_interval[interval]))
    
  def _retrieve(self, stream, start_id, end_time, configuration):
    width = configuration.get('width', self.default_width)
    start_time = self.floor_mod((start_id.time - 0x01b21dd213814000L) * 100/1e9,
                                width)
    end_time = self.ceil_mod(end_time, width)
    keys_to_union = []
    while start_time <= end_time:
      keys_to_union.append('%s:%s:%s' % (self.key_prefix, stream, start_time))
      start_time += width
    tmp_key = '%s:%s:tmp:%s' % (self.key_prefix, stream, random.random())
    # TODO(usmanm): 3 RTs? Should we LUA this and make it faster?
    self.connection.bitop('OR', tmp_key, **keys_to_union)
    bitset = self.connection.get(tmp_key)
    self.connection.delete(tmp_key)
    yield { 'value': bitset }
