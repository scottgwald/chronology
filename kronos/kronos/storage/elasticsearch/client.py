import os

from elasticsearch import Elasticsearch
from elasticsearch import helpers as es_helpers

from kronos.conf.constants import ID_FIELD, TIMESTAMP_FIELD
from kronos.conf.constants import ResultOrder
from kronos.storage.base import BaseStorage
from kronos.utils.math import uuid_to_kronos_time
from kronos.utils.validate import is_non_empty_str, is_pos_int, is_bool, is_list


DOT = u'\uFF0E'
MAX_LIMIT = (1 << 24) - 1
INDEX_TEMPLATE = 'index.template'


class ElasticSearchStorage(BaseStorage):
    SETTINGS_VALIDATORS = {
        'hosts': lambda x: is_list,
        'keyspace_prefix': is_non_empty_str,
        'event_index_template': is_non_empty_str,
        'event_index_prefix': is_non_empty_str,
        'rollover_size': is_pos_int,
        'rollover_check_period_seconds': is_pos_int,
        'read_size': is_pos_int,
        'default_max_items': is_pos_int,
        'force_refresh': is_bool,
    }

    def __init__(self, name, **settings):
        super(ElasticSearchStorage, self).__init__(name, **settings)
        self.force_refresh = settings['force_refresh']

        self.setup_elasticsearch()

    def setup_elasticsearch(self):
        self.es = Elasticsearch(hosts=self.hosts)

        # Load index template.
        template_path = os.path.join(os.path.dirname(__file__), INDEX_TEMPLATE)
        with open(template_path) as f:
            template = f.read()
            template = template.replace('{{ id_field }}', ID_FIELD)
            template = template.replace(
                '{{ timestamp_field }}', TIMESTAMP_FIELD)
            template = template.replace('{{ event_index_prefix }}',
                                        self.event_index_prefix)

            # Always update template (in case it's missing, or it was updated).
            self.es.indices.put_template(
                name=self.event_index_template, body=template)

    def is_alive(self):
        return self.es.ping()

    def transform_event(self, event, insert=False):
        '''
        Recursively cleans keys in ``event`` by replacing `.` with its Unicode full
        width equivalent. ElasticSearch uses dot notation for naming nested fields
        and so having dots in field names can potentially lead to issues. (Note that
        Shay Bannon says field names with dots should be avoided even though
        they *will work* :/)
        '''
        for key in event.keys():
            if insert:
                new_key = key.replace('.', DOT)
            else:
                new_key = key.replace(DOT, '.')
            if isinstance(event[key], dict):
                event[new_key] = self.transform_events(event[key])
            else:
                event[new_key] = event[key]
            if new_key != key:
                del event[key]
        return event

    def _insert(self, namespace, stream, events, configuration):
        """
        `namespace` acts as db for different streams
        `stream` is the name of a stream and `events` is a list of events to
        insert.
        """
        for event in events:
            event = self.transform_event(event, insert=True)
            event['_index'] = namespace
            event['_type'] = stream

        es_helpers.bulk(self.es, events, refresh=self.force_refresh)

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
                        'and': [
                              {'range': {
                                  TIMESTAMP_FIELD: {
                                      'gte': start_time,
                                      'lte': end_time,
                                  }
                              }
                              },
                            {'not': {
                                'term': {
                                    ID_FIELD: str(start_id)
                                }
                            }
                              }
                        ]
                    }
                }
            }
        }
        query = {'index': namespace,
                 'doc_type': stream,
                 'body': body_query,
                 'ignore': 404,
                 'ignore_indices': True
                 }
        # TODO elastic search does not return stats on
        # deletions, https://github.com/elasticsearch/elasticsearch/issues/6519
        count = self.es.search(**query).get('hits', {}).get('total', 0)
        if count:
            self.es.delete_by_query(**query)
        return count

    def _retrieve(self, namespace, stream, start_id,
                  end_time, order, limit, configuration):
        """
        Yield events from stream starting after the event with id `start_id` until
        and including events with timestamp `end_time`.
        """

        start_time = uuid_to_kronos_time(start_id)
        body_query = {
            'query': {
                'filtered': {
                    'query': {'match_all': {}},
                    'filter': {
                        'range': {
                            TIMESTAMP_FIELD: {
                                'gte': start_time,
                                'lte': end_time,
                            }
                        }
                    }
                }
            }
        }
        sort_query = [
            "%s:%s" % (TIMESTAMP_FIELD, ResultOrder.get_short_name(order)), ID_FIELD]

        fetched_count = 0
        while True:
            limit_chunk = min(limit, MAX_LIMIT)
            res = self.es.search(index=namespace,
                                 doc_type=stream,
                                 size=limit_chunk,
                                 body=body_query,
                                 sort=sort_query,
                                 from_=fetched_count,
                                 ignore=[400, 404],
                                 ignore_indices=True)
            hits = res.get('hits', {}).get('hits')
            if not hits:
                return

            if hits[0]['_source'][ID_FIELD] == str(start_id):
                hits = hits[1:]
                fetched_count += 1

            for hit in hits:
                event = hit['_source']
                fetched_count += 1
                if (event[TIMESTAMP_FIELD], event[ID_FIELD]) <= (start_time, start_id):
                    continue
                yield self.transform_event(event)

            limit -= limit_chunk
            if limit == 0:
                return

    def _streams(self, namespace):
        res = self.es.indices.get_mapping(index=namespace,
                                          ignore=404,
                                          allow_no_indices=True,
                                          ignore_unavailable=True)
        if namespace not in res:
            return
        streams = res[namespace]['mappings']
        for key in streams.iterkeys():
            if key != "_default_":
                yield key

    def _clear(self):
        self.es.indices.delete(index='_all')
