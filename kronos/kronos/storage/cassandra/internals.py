import cassandra
import heapq
import json
import random

from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.query import BatchType
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement
from collections import defaultdict
from datetime import timedelta
from types import StringTypes
from uuid import UUID

from kronos.common.cache import InMemoryLRUCache
from kronos.conf.constants import ResultOrder
from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.storage.cassandra.errors import CassandraStorageError
from kronos.storage.cassandra.errors import InvalidStreamComparison
from kronos.storage.cassandra.errors import InvalidTimeUUIDComparison
from kronos.utils.math import round_down
from kronos.utils.math import uuid_to_kronos_time

# Since the Datastax driver doesn't let us pass kwargs to session.prepare
# we'll just go ahead and monkey patch it to work for us.
_prepare = Session.prepare
def patched_prepare(self, query, **kwargs):
  stmt = _prepare(self, query)
  for key, value in kwargs.iteritems():
    setattr(stmt, key, value)
  return stmt
Session.prepare = patched_prepare


class TimeUUID(UUID):
  """
  Override Python's UUID comparator so that time is the first parameter used
  for sorting. This is the comparator behavior for `timeuuid` types in
  Cassandra.
  """
  def __init__(self, *args, **kwargs):
    """
    `order`[kwarg]: Whether to return the results in
             ResultOrder.ASCENDING or ResultOrder.DESCENDING
             time-order.
    """
    # TODO(marcua): Couldn't get `order` to be a named arg (because of
    # subclassing?).  I don't like the next line.
    order = kwargs.pop('order', ResultOrder.ASCENDING)
    super(TimeUUID, self).__init__(*args, **kwargs)

    self._kronos_time = uuid_to_kronos_time(self)
    self._cmp_multiplier = ResultOrder.get_multiplier(order)
    
  def __setattr__(self, name, value):
    # Override UUID's __setattr__ method to make it mutable.
    super(UUID, self).__setattr__(name, value)

  def __cmp__(self, other):
    if other is None:
      return 1
    if isinstance(other, StringTypes):
      try:
        other = UUID(other)
      except (ValueError, AttributeError):
        return 1
    if isinstance(other, StreamEvent):
      other = other.id
    if isinstance(other, UUID):
      return self._cmp_multiplier * cmp((self.time, self.bytes),
                                        (other.time, other.bytes))
    raise InvalidTimeUUIDComparison('Compared TimeUUID to type {0}'
                                    .format(type(other)))


class StreamEvent(object):
  def __init__(self, event, stream_shard):
    if isinstance(event, StringTypes):
      event = json.loads(event)
    self.dict = event
    self.stream_shard = stream_shard
    self.id = TimeUUID(event[ID_FIELD], order=stream_shard.order)

    self._cmp_value = (ResultOrder.get_multiplier(stream_shard.order) *
                       event[TIMESTAMP_FIELD])

  def __cmp__(self, other):
    if not other:
      return 1
    if isinstance(other, TimeUUID):
      return cmp(self.id, other)
    if isinstance(other, StreamEvent):
      return cmp(self.id, other.id)
    if isinstance(other, UUID) or isinstance(other, StringTypes):
      return cmp(self.id, TimeUUID(str(other)))
    raise InvalidTimeUUIDComparison('Compared TimeUUID to type {0}'
                                    .format(type(other)))


class StreamShard(object):
  # CQL commands.
  SELECT_CQL = """SELECT blob FROM stream WHERE
    key = ? AND
    id >= ? AND
    id <= ?
    ORDER BY id
    %s"""
  
  def __init__(self, session, stream, start_time, width, shard, order,
               read_size):
    self.session = session
    self.order = order
    self.read_size = read_size
    self.key = StreamShard.get_key(stream, start_time, shard)

    # If we want to sort in descending order, compare the end of the
    # interval.
    self._cmp_value = ResultOrder.get_multiplier(order) * start_time
    if order == ResultOrder.DESCENDING:
      self._cmp_value -= width

    self.select_cql = None

  @staticmethod
  def get_key(stream, start_time, shard):
    return '%s:%d:%d' % (stream, start_time, shard)

  def __cmp__(self, other):
    if other is None:
      return 1
    elif isinstance(other, StreamShard):
      return cmp((self._cmp_value, self.key), (other._cmp_value, other.key))
    try:
      return cmp(self._cmp_value, float(other))
    except ValueError:
      raise InvalidStreamComparison('Compared StreamShard to type {0}'
                                    .format(type(other)))

  def iterator(self, start_id, end_id):
    if self.order == ResultOrder.DESCENDING:
      order = 'DESC'
    else:
      order = 'ASC'

    if not self.select_cql:
      self.select_cql = self.session.prepare(
        StreamShard.SELECT_CQL % order,
        _routing_key='key',
        fetch_size=self.read_size or 2000,
        consistency_level=ConsistencyLevel.ONE)
    events = self.session.execute(self.select_cql, (self.key, start_id, end_id))
    for event in events:
      try:
        yield StreamEvent(event[0], self)
      except GeneratorExit:
        break
      except ValueError: # Malformed blob?
        pass

    raise StopIteration


class Stream(object):
  # 6 months.
  MAX_WIDTH = int(timedelta(days=365.25).total_seconds() * 1e7) / 2

  # CQL statements.
  DELETE_CQL = """DELETE FROM stream WHERE
    key = ? AND
    id = ?"""
  INSERT_CQL = """INSERT INTO stream (key, id, blob)
    VALUES (?, ?, ?)"""
  INDEX_INSERT_CQL = """INSERT INTO idx (stream, start_time, width, shard)
    VALUES (?, ?, ?, ?)"""
  INDEX_SCAN_CQL = """SELECT start_time, width, shard FROM idx WHERE
    stream = ? AND
    start_time >= ? AND
    start_time < ?"""

  def __init__(self, session, stream, width, shards, read_size):
    self.session = session
    self.read_size = read_size
    self.stream = stream
    self.shards = shards
    self.width = width

    # Index cache is a write cache: it prevents us from writing to the
    # bucket index if we've already updated it in a previous
    # operation.
    self.index_cache = InMemoryLRUCache(max_items=1000)

    # CQL statements (lazily prepared).
    self.delete_cql = None
    self.insert_cql = None
    self.index_insert_cql = None
    self.index_scan_cql = None

  def get_overlapping_shards(self, start_time, end_time):
    if not self.index_scan_cql:
      self.index_scan_cql = self.session.prepare(Stream.INDEX_SCAN_CQL,
                                                 _routing_key='stream')

    potential_shards = self.session.execute(
      self.index_scan_cql,
      (self.stream, max(start_time - Stream.MAX_WIDTH, 0), end_time))
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
    self.insert_to_batch(batch_stmt, events)
    self.session.execute(batch_stmt)

  def insert_to_batch(self, batch_stmt, events):
    if not (self.insert_cql or self.index_insert_cql):
      self.insert_cql = self.session.prepare(
        Stream.INSERT_CQL,
        _routing_key='key',
        consistency_level=ConsistencyLevel.QUORUM)
      self.index_insert_cql = self.session.prepare(
        Stream.INDEX_INSERT_CQL,
        _routing_key='stream',
        consistency_level=ConsistencyLevel.QUORUM)

    shard_idx = {}
    for event in events:
      shard_time = round_down(event[TIMESTAMP_FIELD], self.width)
      shard = shard_idx.get(shard_time,
                            random.randint(0, self.shards - 1))
      
      # Insert to index.
      try:
        self.index_cache.get((shard_time, shard))
      except KeyError:
        bound_stmt = self.index_insert_cql.bind(
          (self.stream, shard_time, self.width, shard))
        batch_stmt.add(bound_stmt)
        self.index_cache.set((shard_time, shard), None)

      # Insert to stream.
      batch_stmt.add(self.insert_cql,
                     (StreamShard.get_key(self.stream, shard_time, shard),
                      TimeUUID(event[ID_FIELD]),
                      json.dumps(event)))
      shard_idx[shard_time] = (shard + 1) % self.shards # Round robin.

  def iterator(self, start_id, end_id, order, limit):
    start_id = TimeUUID(str(start_id), order=order)
    end_id = TimeUUID(str(end_id), order=order)
    
    shards = self.get_overlapping_shards(start_id._kronos_time,
                                         end_id._kronos_time)
    shards = sorted(map(lambda shard: StreamShard(self.session,
                                                    self.stream,
                                                    shard['start_time'],
                                                    shard['width'],
                                                    shard['shard'],
                                                    order,
                                                    self.read_size), shards))

    iterators = {}
    event_heap = []

    def load_next_shard():
      """
      Pulls the earliest event from the next earliest shard and puts it into the
      event heap.
      """
      if not shards:
        return
      shard = shards.pop(0)
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
        load_next_shard()

      if event_heap:
        # Pull events from all shards that overlap with the next event to be
        # yielded.
        top_event = event_heap[0]
        while shards:
          if top_event._cmp_value < shards[0]._cmp_value:
            break
          load_next_shard()
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
          # Note: in ResultOrder.DESCENDING conditions below, we flip `<` for
          # `>` and `>=` for `<=` UUID comparator logic is flipped.
          if ((order == ResultOrder.ASCENDING and end_id < event) or
              (order == ResultOrder.DESCENDING and start_id < event)):
            raise StopIteration
          elif ((order == ResultOrder.ASCENDING and start_id <= event) or
                (order == ResultOrder.DESCENDING and end_id <= event)):
            limit -= 1
            yield event

        load_overlapping_shards()

    for event in _iterator(limit):
      yield event.dict
    
  def delete(self, start_id, end_id):
    batch_stmt = BatchStatement(batch_type=BatchType.UNLOGGED,
                                consistency_level=ConsistencyLevel.QUORUM)
    num_deleted = self.delete_to_batch(batch_stmt, start_id, end_id)
    self.session.execute(batch_stmt)
    return num_deleted
  
  def delete_to_batch(self, batch_stmt, start_id, end_id):
    if not self.delete_cql:
      self.delete_cql = self.session.prepare(Stream.DELETE_CQL,
                                             _routing_key='key')
    
    start_id = TimeUUID(str(start_id))
    end_id = TimeUUID(str(end_id))
    shards = self.get_overlapping_shards(start_id._kronos_time,
                                         end_id._kronos_time)
    num_deleted = 0
    for shard in shards:
      shard = StreamShard(self.session, self.stream,
                            shard['start_time'], shard['width'],
                            shard['shard'], ResultOrder.ASCENDING,
                            read_size=self.read_size)
      for event in shard.iterator(start_id, end_id):
        if event.id == start_id:
          continue
        num_deleted += 1
        batch_stmt.add(self.delete_cql, (shard.key,
                                         TimeUUID(event.dict[ID_FIELD])))
    return num_deleted


class Namespace(object): 
  # CQL commands.
  CREATE_NAMESPACE_CQL = """CREATE KEYSPACE %s WITH
    REPLICATION = {'class': 'SimpleStrategy',
                   'replication_factor': %d}"""
  DROP_NAMESPACE_CQL = """DROP KEYSPACE %s"""
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
      stream = Stream(self.session, stream_name, width, shards, self.read_size)
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
      session.execute(Namespace.CREATE_NAMESPACE_CQL %
                      (self.name, self.replication_factor))
      session.set_keyspace(self.name)

      # Create column families + indices.
      session.execute(Namespace.STREAM_CQL)
      session.execute(Namespace.INDEX_CQL)

    self.session = session

  def destroy_session(self):
    self.session.shutdown()
    self.session = None

  def drop(self):
    self.session.execute(Namespace.DROP_NAMESPACE_CQL % self.name)
    # Session should automatically expire if keyspace dropped.
    self.destroy_session()
