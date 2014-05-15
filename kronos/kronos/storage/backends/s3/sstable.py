import bisect
import boto
import json
import tempfile

from kronos.utils.math import round_down


class S3SSTableError(Exception):
  pass


class S3SSTable(object):
  def __init__(self, s3_connection, bucket_name, key_name, block_size,
               index_file_threshold):
    self.connection = s3_connection
    self.index_file_threshold = index_file_threshold
    self.block_size = block_size

    self.bucket_name = bucket_name
    self.key_name = key_name
    key = boto.s3.Bucket(s3_connection, bucket_name).get_key(key_name)
    if key.exists():
      self.size = int(self.key.get_metadata('Content-Length'))
    else:
      self.size = 0

    if self.has_index_file():
      self.index = self.bucket.get_key('%s_index' % key_name)
    else:
      self.index = None
    self._cached_index = None

  def has_index_file(self, size=None):
    if size is None:
      size = self.size
    return round_down(self.size, self.block_size) > self.index_file_threshold

  def _fetch_index(self):
    if not self.has_index_file() or self._cached_index:
      return

    cached_index = map(lambda x: x.split(':'),
                       self.index.get_contents_as_string().split('\n'))
    self._cached_index = {'time': map(lambda x: int(x[0]), cached_index),
                          'offset': map(lambda x: int(x[1]), cached_index)}

  def iterator(self, start_time=None, end_time=None, reverse=False):
    self._fetch_index()

    if start_time is None:
      start_time = 0
    if end_time is None:
      end_time = 0xfffffffffffffffff # Max time.

    if self._cached_index:
      i = bisect.bisect_left(self._cached_index['time'], start_time)
      if i != 0:
        i -= 1
      min_byte = self._cached_index['offset'][i]
      i = bisect.bisect_right(self._cached_index['time'], end_time)
      if i < len(self._cached_index['time']) - 1:
        i += 1
      max_byte = self._cached_index['offset'][i]
    else:
      min_byte = 0
      max_byte = self.size

    with tempfile.NamedTemporaryFile() as tmp_file:
      response = self.connection.make_request(
        'GET',
        bucket=self.bucket_name,
        key=self.key_name,
        headers={'Range': 'bytes=%d-%d' % (min_byte, max_byte)}
        ).read().split('\n')

      response_size = 0
      while True:
        data = response.read(5092)
        if not data:
          break
        tmp_file.write(data)
        response_size += len(data)

      assert response_size == self.size

      

  def insert_events(self, events):
    if self.size:
      raise S3SSTableError('S3SSTable files are immutable.')

    index = {}
    size = 0
    with tempfile.NamedTemporaryFile() as tmp_file:
      blocks = 0
      for event in events:
        content = json.dumps(event)
        tmp_file.write(content)
        tmp_file.write('\n')
        new_size = size + len(content) + 1
        blocks += round_down(new_size, self.block_size) % len(index)
        if blocks not in index:
          index[blocks] = (event['@time'], size)
        size = new_size
      self.key.set_contents_from_file(tmp_file)

    if self.has_index_file(size):
      cached_index = sorted(index.values(), key=lambda x: x[0])
      with tempfile.NamedTemporaryFile() as tmp_file:
        for data in cached_index:
          tmp_file.write('%d:%d' % data)
          tmp_file.write('\n')
        self.index.set_contents_from_file(tmp_file)
      self._cached_index = {'time': map(lambda x: x[0], cached_index),
                            'offset': map(lambda x: x[1], cached_index)}

    self.size = size
