import cassandra
import heapq
import random

from cassandra import ConsistencyLevel
from cassandra import cqltypes
from cassandra.protocol import ResultMessage
from cassandra.query import BatchType
from cassandra.query import BatchStatement
from cassandra.query import BoundStatement
from cassandra.query import SimpleStatement
from collections import defaultdict
from datetime import timedelta
from timeuuid import TimeUUID

from kronos.common.cache import InMemoryLRUCache
from kronos.conf.constants import MAX_LIMIT
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.core import marshal
from kronos.core.errors import InvalidTimeUUIDComparison
from kronos.core.executor import execute_greenlet_async
from kronos.core.executor import wait
from kronos.storage.cassandra.errors import CassandraStorageError
from kronos.storage.cassandra.errors import InvalidStreamComparison
from kronos.utils.math import round_down
from kronos.utils.uuid import UUIDType
from kronos.utils.uuid import uuid_from_kronos_time
from kronos.utils.uuid import uuid_to_kronos_time


# Patch Datastax driver to return TimeUUID objects rather than plain UUID
# objects if the CQL type is timeuuid.
class TimeUUIDType(cqltypes.TimeUUIDType):
  @staticmethod
  def deserialize(byts, protocol_version):
    return TimeUUID(bytes=byts)

ResultMessage._type_codes[0x000F] = TimeUUIDType


class StreamEvent(object):
  def __init__(self, _id, event_json, stream_shard):
    self.stream_shard = stream_shard
    self.json = event_json
    _id.descending = stream_shard.descending
    self.id = _id

  def __cmp__(self, other):
    if not other:
      return 1
    if isinstance(other, TimeUUID):
      return cmp(self.id, other)
    if isinstance(other, StreamEvent):
      return cmp(self.id, other.id)
    raise InvalidTimeUUIDComparison('Compared TimeUUID to type {0}'
                                    .format(type(other)))


class StreamShard(object):
  def __init__(self, namespace, stream, start_time, width, shard, descending,
               limit, read_size):
    self.session = namespace.session
    self.descending = descending
    self.read_size = read_size
    self.limit = limit
    self.key = StreamShard.get_key(stream, start_time, shard)
    self.namespace = namespace

    # If we want to sort in descending order, compare the end of the
    # interval.
    if descending:
      self.cmp_id = uuid_from_kronos_time(start_time + width, UUIDType.HIGHEST)
      self.cmp_id.descending = True
    else:
      self.cmp_id = uuid_from_kronos_time(start_time, UUIDType.LOWEST)

    self._events_future = None

  @staticmethod
  def get_key(stream, start_time, shard):
    return '%s:%d:%d' % (stream, start_time, shard)

  def __cmp__(self, other):
    if other is None:
      return 1
    elif isinstance(other, StreamShard):
      return cmp((self.cmp_id, self.key), (other.cmp_id, other.key))
    raise InvalidStreamComparison('Compared StreamShard to type {0}'
                                  .format(type(other)))

  def start_fetching_events_async(self, start_id, end_id):
    if self.descending:
      select_stmt = self.namespace.SELECT_DESC_STMT
    else:
      select_stmt = self.namespace.SELECT_ASC_STMT

    select_stmt = BoundStatement(select_stmt,
                                 fetch_size=self.read_size or 5000,
                                 routing_key=self.key,
                                 consistency_level=ConsistencyLevel.ONE)
    self._events_future = self.session.execute_async(
      select_stmt.bind((self.key, start_id, end_id, self.limit)))

  def iterator(self, start_id=None, end_id=None):
    if self._events_future is None:
      assert start_id is not None and end_id is not None
      self.start_fetching_events_async(start_id, end_id)
    for event in self._events_future.result():
      try:
        yield StreamEvent(event[0], event[1], self)
      except GeneratorExit:
        break

  def ids_iterator(self, start_id, end_id):
    select_ids_stmt = BoundStatement(self.namespace.SELECT_ID_STMT,
                                     fetch_size=100000, # 100k ids at a time.
                                     routing_key=self.key,
                                     consistency_level=ConsistencyLevel.ONE)
    ids = self.session.execute(
      select_ids_stmt.bind((self.key, start_id, end_id, self.limit)))
    for _id in ids:
      try:
        yield _id[0]
      except GeneratorExit:
        break


class Stream(object):
  # 6 months.
  MAX_WIDTH = int(timedelta(days=365.25).total_seconds() * 1e7) / 2

  def __init__(self, namespace, stream, width, shards, read_size):
    self.session = namespace.session
    self.read_size = read_size
    self.stream = stream
    self.shards = shards
    self.width = width
    self.namespace = namespace

    # Index cache is a write cache: it prevents us from writing to the
    # bucket index if we've already updated it in a previous
    # operation.
    self.index_cache = InMemoryLRUCache(max_items=1000)

  def get_overlapping_shards(self, start_time, end_time):
    index_scan_stmt = BoundStatement(self.namespace.INDEX_SCAN_STMT)
    potential_shards = self.session.execute(
      index_scan_stmt.bind((self.stream,
                            max(start_time - Stream.MAX_WIDTH, 0),
                            end_time))
      )
    shards = defaultdict(lambda: defaultdict(int))
    for (shard_time, width, shard) in potential_shards:
      if shard_time + width < start_time:
        # end_time < shard start_time?
        continue
      shards[shard_time][shard] = max(shards[shard_time][shard], width)
    for shard_time, _ in shards.iteritems():
      for shard, width in _.iteritems():
        yield {'start_time': shard_time,
               'width': width,
               'shard': shard}

  def insert(self, events):
    if not events:
      return
    batch_stmt = BatchStatement(batch_type=BatchType.UNLOGGED,
                                consistency_level=ConsistencyLevel.QUORUM)

    shard_idx = {}
    for _id, event in events:
      shard_time = round_down(event[TIMESTAMP_FIELD], self.width)
      shard = shard_idx.get(shard_time,
                            random.randint(0, self.shards - 1))
      
      # Insert to index.
      try:
        self.index_cache.get((shard_time, shard))
      except KeyError:
        batch_stmt.add(BoundStatement(self.namespace.INDEX_INSERT_STMT,
                                      routing_key=self.stream,
                                      consistency_level=ConsistencyLevel.QUORUM)
                       .bind((self.stream, shard_time, self.width, shard)))
        self.index_cache.set((shard_time, shard), None)

      # Insert to stream.
      shard_key = StreamShard.get_key(self.stream, shard_time, shard)
      batch_stmt.add(BoundStatement(self.namespace.INSERT_STMT,
                                    routing_key=shard_key,
                                    consistency_level=ConsistencyLevel.QUORUM)
                     .bind((shard_key,
                            _id,
                            marshal.dumps(event))))
      shard_idx[shard_time] = (shard + 1) % self.shards # Round robin.

    self.session.execute(batch_stmt)

  def iterator(self, start_id, end_id, descending, limit):
    start_id.descending = descending
    end_id.descending = descending
    
    shards = self.get_overlapping_shards(uuid_to_kronos_time(start_id),
                                         uuid_to_kronos_time(end_id))
    shards = sorted(map(lambda shard: StreamShard(self.namespace,
                                                  self.stream,
                                                  shard['start_time'],
                                                  shard['width'],
                                                  shard['shard'],
                                                  descending,
                                                  limit,
                                                  self.read_size),
                        shards))
    iterators = {}
    event_heap = []
    shards_to_load = []

    def load_next_shards(cmp_id):
      """
      Pulls the earliest event from the next earliest shard and puts it into the
      event heap.
      """
      while shards and shards[0].cmp_id <= cmp_id:
        shard = shards.pop(0)
        shard.start_fetching_events_async(start_id, end_id)
        shards_to_load.append(shard)
      while shards_to_load:
        shard = shards_to_load.pop(0)
        it = shard.iterator(start_id, end_id)
        try:
          event = it.next()
          heapq.heappush(event_heap, event)
          iterators[shard] = it
        except StopIteration:
          pass

    def load_overlapping_shards():
      """
      Given what the current most recently loaded event is, loads any
      shards that might overlap with that event. Multiple shards
      might overlap because they have overlapping time slices.
      """
      while not event_heap and shards:
        # Try to pull events from unread shards.
        load_next_shards(shards[0].cmp_id)

      if event_heap and shards:
        # Pull events from all shards that overlap with the next event to be
        # yielded.
        load_next_shards(event_heap[0].id)
      elif not iterators:
        # No events in the heap and no active iterators? We're done!
        return
        
      shards_with_events = set(event.stream_shard for event in event_heap)
      for shard in iterators.keys():
        if shard in shards_with_events:
          continue
        try:
          it = iterators[shard]
          event = it.next()
          heapq.heappush(event_heap, event)
        except StopIteration:
          del iterators[shard]
    
    def _iterator(limit):
      load_overlapping_shards() # bootstrap.

      # No events?
      if not event_heap:
        raise StopIteration

      while event_heap or shards:
        if limit <= 0:
          raise StopIteration
        if event_heap:
          # Get the next event to return.
          event = heapq.heappop(event_heap)
          # Note: in descending conditions below, we flip `<` for
          # `>` and `>=` for `<=` UUID comparator logic is flipped.
          if ((not descending and event.id > end_id) or
              (descending and event.id > start_id)):
            raise StopIteration
          elif ((not descending and event.id >= start_id) or
                (descending and event.id >= end_id)):
            limit -= 1
            yield event

        load_overlapping_shards()

    for event in _iterator(limit):
      yield event
    
  def delete(self, start_id, end_id):
    shards = list(self.get_overlapping_shards(uuid_to_kronos_time(start_id),
                                              uuid_to_kronos_time(end_id)))
    def delete_from_shard(shard):
      batch_stmt = BatchStatement(batch_type=BatchType.UNLOGGED,
                                  consistency_level=ConsistencyLevel.QUORUM)
      num_deleted = 0
      shard = StreamShard(self.namespace, self.stream,
                          shard['start_time'], shard['width'],
                          shard['shard'], False,
                          MAX_LIMIT, read_size=self.read_size)
      for _id in shard.ids_iterator(start_id, end_id):
        if _id == start_id:
          continue
        num_deleted += 1
        batch_stmt.add(BoundStatement(self.namespace.DELETE_STMT,
                                      routing_key=shard.key,
                                      consistency_level=ConsistencyLevel.QUORUM)
                       .bind((shard.key, _id)))
      self.session.execute(batch_stmt)
      return num_deleted
    
    for i, shard in enumerate(shards):
      shards[i] = execute_greenlet_async(delete_from_shard, shard)
    wait(shards)

    errors = []
    num_deleted = 0
    for shard in shards:
      try:
        num_deleted += shard.get()
      except Exception, e:
        errors.append(repr(e))

    return num_deleted, errors


class Namespace(object): 
  # Namespace-level CQL statements.
  CREATE_KEYSPACE_CQL = """CREATE KEYSPACE %s WITH
    REPLICATION = {'class': 'SimpleStrategy',
                   'replication_factor': %d}"""
  DROP_KEYSPACE_CQL = """DROP KEYSPACE %s"""
  # key is of the form: "<stream_name>:<start_time>:<time_width>"
  STREAM_CQL = """CREATE TABLE stream (
    key text,
    id timeuuid,
    blob text,
    PRIMARY KEY (key, id)
  )"""
  INDEX_CQL = """CREATE TABLE idx (
    stream text,
    start_time bigint,
    width bigint,
    shard int,
    PRIMARY KEY (stream, start_time, width, shard)
  )"""
  STREAM_LIST_CQL = """SELECT DISTINCT stream FROM idx"""

  # Stream-level CQL statements.
  DELETE_STMT = """DELETE FROM stream WHERE
    key = ? AND
    id = ?"""
  INSERT_STMT = """INSERT INTO stream (key, id, blob)
    VALUES (?, ?, ?)"""
  INDEX_INSERT_STMT = """INSERT INTO idx (stream, start_time, width, shard)
    VALUES (?, ?, ?, ?)"""
  INDEX_SCAN_STMT = """SELECT start_time, width, shard FROM idx WHERE
    stream = ? AND
    start_time >= ? AND
    start_time < ?"""

  # StreamShard-level CQL statements.
  SELECT_ASC_STMT = """SELECT id, blob FROM stream WHERE
    key = ? AND
    id >= ? AND
    id <= ?
    ORDER BY id ASC
    LIMIT ?
    """
  SELECT_DESC_STMT = """SELECT id, blob FROM stream WHERE
    key = ? AND
    id >= ? AND
    id <= ?
    ORDER BY id DESC
    LIMIT ?
    """
  SELECT_ID_STMT = """SELECT id FROM stream WHERE
    key = ? AND
    id >= ? AND
    id <= ?
    LIMIT ?
    """

  def __init__(self, cluster, name, replication_factor, read_size):
    self.cluster = cluster
    self.name = name
    self.replication_factor = replication_factor
    self.read_size = read_size
    self.session = None

    # Create session.
    self.create_session()

    # Cache for Stream instances.
    self.stream_cache = InMemoryLRUCache(max_items=1000)

  def get_stream(self, stream_name, width, shards):
    # width and shard settings change requires a restart of kronosd, so we can
    # just cache on stream name.
    try:
      return self.stream_cache.get(stream_name)
    except KeyError:
      stream = Stream(self, stream_name, width, shards, self.read_size)
      self.stream_cache.set(stream_name, stream)
      return stream

  def list_streams(self):
    for stream in self.session.execute(
      SimpleStatement(Namespace.STREAM_LIST_CQL,
                      consistency_level=ConsistencyLevel.QUORUM)):
      yield stream[0]

  def create_session(self):
    if self.session:
      raise CassandraStorageError

    try:
      session = self.cluster.connect(self.name)
    except cassandra.InvalidRequest, e:
      if "Keyspace '%s' does not exist" % self.name not in e.message:
        raise e
      session = self.cluster.connect()

      # Create keyspace for namespace.
      session.execute(Namespace.CREATE_KEYSPACE_CQL %
                      (self.name, self.replication_factor))
      session.set_keyspace(self.name)

      # Create column families + indices.
      session.execute(Namespace.STREAM_CQL)
      session.execute(Namespace.INDEX_CQL)

    self.session = session

    # Prepare statements for this session.
    for attr, value in Namespace.__dict__.iteritems():
      if not (attr.upper() == attr and attr.endswith('_STMT')):
        continue
      setattr(self, attr, self.session.prepare(value))

  def destroy_session(self):
    self.session.shutdown()
    self.session = None

  def drop(self):
    self.session.execute(Namespace.DROP_KEYSPACE_CQL % self.name)
    # Session should automatically expire if keyspace dropped.
    self.destroy_session()
