import atexit
import gevent
import os
import weakref

from datetime import datetime
from dateutil.tz import tzutc
from elasticsearch import Elasticsearch
from elasticsearch import helpers as es_helpers
from elasticsearch.exceptions import TransportError
from timeuuid import TimeUUID
from timeuuid import UUIDType
from uuid import uuid4

from kronos.common.time import kronos_time_to_datetime
from kronos.conf.constants import ID_FIELD, TIMESTAMP_FIELD
from kronos.conf.constants import ResultOrder
from kronos.core import marshal; json = marshal.get_marshaler('json')
from kronos.storage.base import BaseStorage
from kronos.utils.math import round_down
from kronos.utils.uuid import uuid_from_kronos_time
from kronos.utils.uuid import uuid_to_kronos_time
from kronos.utils.validate import is_bool
from kronos.utils.validate import is_int
from kronos.utils.validate import is_list
from kronos.utils.validate import is_non_empty_str
from kronos.utils.validate import is_pos_int


INDEX_TEMPLATE = 'index.template'
INDEX_PATTERN = '%Y.%m.%d.%H' # YYYY.MM.DD.HH for Kibana.
LOGSTASH_TIMESTAMP_FIELD = '@timestamp'


def _round_datetime_down(dt, interval):
  kwargs = {'tzinfo': tzutc(),
            'microsecond': 0,
            'second': 0,
            'minute': 0,
            'hour': 0,
            'day': 1,
            'month': 1,
            'year': dt.year}
  if interval < IndexInterval.YEAR:
    kwargs['month'] = dt.month
  if interval < IndexInterval.WEEK:
    kwargs['day'] = dt.day
  elif interval == IndexInterval.WEEK:
    kwargs['day'] = round_down(dt.day, 7)
  if interval < IndexInterval.DAY:
    kwargs['hour'] = dt.hour
  return datetime(**kwargs)


class IndexInterval(object):
  """
  Index *intervals* supported by Logstash/Kibana.
  http://www.elasticsearch.org/guide/en/kibana/current/_dashboard_schema.html#index-settings
  """
  HOUR = 0
  DAY = 1
  WEEK = 2
  MONTH = 3
  YEAR = 4


class IndexManager(object):
  def __init__(self, storage):
    self.es = storage.es
    self.index_prefix = storage.index_prefix
    self.rollover_size = storage.rollover_size
    self.rollover_check_period_seconds = storage.rollover_check_period_seconds
    self.namespaces = storage.namespaces
    self.namespace_to_metadata = {}

    self.update()
    
    self.rollover_worker = gevent.spawn(self.update_periodic)
    atexit.register(self.kill_rollover_worker)

  def kill_rollover_worker(self):
    if not self.rollover_worker.ready():
      self.rollover_worker.kill()
    
  def update_periodic(self):
    while True:
      try:
        gevent.sleep(self.rollover_check_period_seconds)
        self.update()
      except gevent.GreenletExit:
        return

  def update(self):
    def rollover_index(namespace, version):
      rand = uuid4().hex
      try:
        doc = self.es.index('%s:dir' % self.index_prefix,
                            'directory',
                            {'rand': rand},
                            id=namespace,
                            version=version,
                            refresh=True)
      except TransportError, e:
        if e.status_code != 409: # VersionConflictEngineException?
          raise
        doc = self.es.get('%s:dir' % self.index_prefix,
                          namespace,
                          doc_type='directory')
      return rand, doc['_version']
    
    docs = self.es.mget({'ids': list(self.namespaces)},
                        index='%s:dir' % self.index_prefix,
                        doc_type='directory')
    for doc in docs['docs']:
      namespace = doc['_id']
      if not doc.get('found'):
        rand, version = rollover_index(namespace, 0)
      else:
        rand, version = doc['_source']['rand'], doc['_version']
      self.namespace_to_metadata[namespace] = [self.get_alias(namespace,
                                                              rand),
                                               version]

    for namespace in self.namespaces:
      index, version = self.namespace_to_metadata[namespace]
      count = self.es.count(index=index,
                            ignore_unavailable=True).get('count', 0)
      if count >= self.rollover_size:
        rand, version = rollover_index(namespace, version)
        self.namespace_to_metadata[namespace] = [self.get_alias(namespace,
                                                               rand),
                                                 version]

  def get_all_indices(self):
    return [index
            for index in self.es.indices.status(index='_all')['indices']
            if index.startswith(self.index_prefix)]

  def add_aliases(self, index, aliases):
    self.es.indices.update_aliases({
      'actions': [{'add': {'index': index, 'alias': alias}}
                  for alias in aliases]
      })

  def get_index(self, namespace):
    return self.namespace_to_metadata[namespace][0]

  def get_alias(self, namespace, dt, interval=None):
    if interval is None:
      assert is_non_empty_str(dt)
      suffix = dt
    else:
      if is_int(dt):
        dt = kronos_time_to_datetime(dt)
      suffix = _round_datetime_down(dt, interval).strftime(INDEX_PATTERN)
    return '%s-%s-%s' % (self.index_prefix,
                         namespace,
                         suffix)

  def get_all_aliases(self, namespace):
    aliases = []
    for key, value in self.es.indices.get_aliases().iteritems():
      if not key.startswith('%s-%s' % (self.index_prefix, namespace)):
        continue
      aliases.extend(value['aliases'])
    return sorted(aliases)

  def get_aliases(self, namespace, start_time, end_time, interval):
    # TODO(usmanm): This will not work with dynamic changes to interval.
    start_alias = self.get_alias(namespace, start_time, interval)
    end_alias = self.get_alias(namespace, end_time, interval)
    return filter(lambda alias: alias >= start_alias and alias <= end_alias,
                  self.get_all_aliases(namespace))


class ElasticSearchStorage(BaseStorage):
  SETTINGS_VALIDATORS = {
    'force_refresh': is_bool,
    'hosts': lambda x: is_list,
    'index_template': is_non_empty_str,
    'index_prefix': is_non_empty_str,
    'index_interval': lambda x: True,
    'replicas': is_int,
    'rollover_size': is_pos_int,
    'rollover_check_period_seconds': is_pos_int,
    'read_size': is_pos_int,
    'shards': is_pos_int,
  }

  def __init__(self, name, namespaces, **settings):
    super(ElasticSearchStorage, self).__init__(name, namespaces, **settings)
    self.setup_elasticsearch()
    self.index_manager = IndexManager(weakref.proxy(self))

  def setup_elasticsearch(self):
    self.es = Elasticsearch(hosts=self.hosts,
                            sniff_on_start=True,
                            sniff_on_connection_fail=True)

    # Load index template.
    template_path = os.path.join(os.path.dirname(__file__), INDEX_TEMPLATE)
    with open(template_path) as f:
      template = f.read()
      for var, value in {'id_field': ID_FIELD,
                         'timestamp_field': TIMESTAMP_FIELD,
                         'index_prefix': self.index_prefix,
                         'shards': self.shards,
                         'replicas': self.replicas}.iteritems():
        template = template.replace('{{ %s }}' % var, str(value))

    # Always update template (in case it's missing, or it was updated).
    self.es.indices.put_template(name=self.index_template, body=template)

  def is_alive(self):
    return self.es.ping()

  def _insert(self, namespace, stream, events, configuration):
    """
    `namespace` acts as db for different streams
    `stream` is the name of a stream and `events` is a list of events to
    insert.
    """
    index = self.index_manager.get_index(namespace)
    aliases_to_add = set()

    def actions():
      for _id, event in events:
        dt = kronos_time_to_datetime(uuid_to_kronos_time(_id))
        aliases_to_add.add(self.index_manager.get_alias(
          namespace,
          dt,
          configuration['index_interval']))
        
        event['_index'] = index
        event['_type'] = stream
        event[LOGSTASH_TIMESTAMP_FIELD] = dt.isoformat()

        yield event

    list(es_helpers.streaming_bulk(self.es, actions(), chunk_size=1000,
                                   refresh=self.force_refresh))
    self.index_manager.add_aliases(index, aliases_to_add)
  
  def _delete(self, namespace, stream, start_id, end_time, configuration):
    """
    Delete events with id > `start_id` and end_time <= `end_time`.
    """
    start_time = uuid_to_kronos_time(start_id)
    body_query = {
      'query': {
        'filtered': {
          'query': {'match_all': {}},
          'filter': {
            'bool': {
              'should': [
                {'range': {TIMESTAMP_FIELD: {'gt': start_time,
                                             'lte': end_time}}},
                {'bool': {
                  'must': [
                    {'range': {ID_FIELD: {'gt': str(start_id)}}},
                    {'term': {TIMESTAMP_FIELD: start_time}}
                    ]
                  }
                 }
                ]
              }
            }
          }
        }
      }
    query = {'index': self.index_manager.get_index(namespace),
             'doc_type': stream,
             'body': body_query,
             'ignore': 404,
             'allow_no_indices': True,
             'ignore_unavailable': True}
    try:
      # XXX: ElasticSearch does not return stats on deletions.
      # https://github.com/elasticsearch/elasticsearch/issues/6519
      count = self.es.count(**query).get('count', 0)
      if count:
        self.es.delete_by_query(**query)
      return count, []
    except Exception, e:
      return 0, [repr(e)]

  def _retrieve(self, namespace, stream, start_id,
                  end_time, order, limit, configuration):
    """
    Yield events from stream starting after the event with id `start_id` until
    and including events with timestamp `end_time`.
    """
    indices = self.index_manager.get_aliases(namespace,
                                             uuid_to_kronos_time(start_id),
                                             end_time,
                                             configuration['index_interval'])
    if not indices:
      return

    end_id = uuid_from_kronos_time(end_time, _type=UUIDType.HIGHEST)
    end_id.descending = start_id.descending = descending = (
      order == ResultOrder.DESCENDING)
    
    start_time = uuid_to_kronos_time(start_id)
    body_query = {
      'query': {
        'filtered': {
          'query': {'match_all': {}},
          'filter': {
            'range': {TIMESTAMP_FIELD: {'gte': start_time, 'lte': end_time}}
            }
          }
        }
      }
    order = 'desc' if descending else 'asc'
    sort_query = [
      '%s:%s' % (TIMESTAMP_FIELD, order),
      '%s:%s' % (ID_FIELD, order)
      ]

    last_id = end_id if descending else start_id
    scroll_id = None
    while True:
      size = max(min(limit, configuration['read_size']) / self.shards, 10)
      if scroll_id is None:
        res = self.es.search(index=indices,
                             doc_type=stream,
                             size=size,
                             body=body_query,
                             sort=sort_query,
                             _source=True,
                             scroll='1m',
                             ignore=[400, 404],
                             allow_no_indices=True,
                             ignore_unavailable=True)
      else:
        res = self.es.scroll(scroll_id, scroll='1m')
      if '_scroll_id' not in res:
        break
      scroll_id = res['_scroll_id']
      hits = res.get('hits', {}).get('hits')
      if not hits:
        break

      for hit in hits:
        _id = TimeUUID(hit['_id'], descending=descending)
        if _id <= last_id:
          continue
        last_id = _id
        event = hit['_source']
        yield json.dumps(event)
        limit -= 1
        if limit == 0:
          break

    if scroll_id is not None:
      self.es.clear_scroll(scroll_id)

  def _streams(self, namespace):
    index = self.index_manager.get_index(namespace)
    res = self.es.indices.get_mapping(index=index,
                                      ignore=404,
                                      allow_no_indices=True,
                                      ignore_unavailable=True)
    if index not in res:
      return
    streams = res[index]['mappings']
    for key in streams:
      if key != '_default_':
        yield key

  def _clear(self):
    self.es.indices.delete(self.index_manager.get_all_indices())

  def stop(self):
    self.index_manager.kill_rollover_worker()
