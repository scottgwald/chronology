import json
import leveldb
import random
import struct
import time


class LevelDBStore(object):
  def __init__(self, path, db_name=None):
    self.path = path
    self.db_name = db_name or str(int(time.time()))
    self.db = leveldb.LevelDB('%s/%s' % (path.rstrip('/'), self.db_name))

  @staticmethod
  def generate_key(event):
    return struct.pack('@QI', event['@time'], random.randint(0, 0xffffffff))

  def insert(self, event):
    self.db.Put(LevelDBStore.generate_key(event), json.dumps(event))

  def scan(self, start_time=None, end_time=None):
    return (
      json.loads(value) for key, value in 
      self.db.CreateSnapshop().RangeIter(
        struct.pack('@QI', start_time or 0, 0),
        struct.pack('@QI', end_time or 0xffffffffffffffff, 0xffffffff))
      )

  def scan_and_purge(self, start_time=None, end_time=None):
    for key, value in self.db.CreateSnapshop().RangeIter(
        struct.pack('@QI', start_time or 0, 0),
        struct.pack('@QI', end_time or 0xffffffffffffffff, 0xffffffff)):
      try:
        self.db.Delete(key)
      except leveldb.LevelDBError:
        pass
      yield json.loads(value)
