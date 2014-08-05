import gevent

from datetime import datetime
from timeuuid import TimeUUID

from kronos.common.time import datetime_to_kronos_time
from kronos.conf import settings
from kronos.conf.constants import ID_FIELD
from kronos.conf.constants import TIMESTAMP_FIELD
from kronos.storage.elasticsearch.client import IndexManager
from kronos.storage.router import router
from tests.server import KronosServerTestCase


def reload_router(kill_update_thread=False):
  router.reload()
  backend = router.get_backend('elasticsearch')
  if kill_update_thread:
    backend.index_manager.kill_rollover_worker()


class TestElasticSearchBackend(KronosServerTestCase):  
  def test_aliasing(self):
    settings.storage.elasticsearch.rollover_size = 10
    reload_router(kill_update_thread=True)
    index1 = router.get_backend('elasticsearch').index_manager.get_index(
      'kronos')

    time1 = datetime_to_kronos_time(datetime(2014, 1, 1, 0))
    time2 = datetime_to_kronos_time(datetime(2014, 1, 1, 1))
    time3 = datetime_to_kronos_time(datetime(2014, 1, 1, 2))
    self.put('test_aliasing', [{TIMESTAMP_FIELD: time1, 'i': i, 'j': 0}
                               for i in xrange(5)])
    self.put('test_aliasing', [{TIMESTAMP_FIELD: time2, 'i': i, 'j': 0}
                               for i in xrange(5)])

    router.get_backend('elasticsearch').index_manager.update()
    index2 = router.get_backend('elasticsearch').index_manager.get_index(
      'kronos')
    
    self.put('test_aliasing', [{TIMESTAMP_FIELD: time2, 'i': i, 'j': 1}
                               for i in xrange(5)])
    self.put('test_aliasing', [{TIMESTAMP_FIELD: time3, 'i': i, 'j': 1}
                               for i in xrange(5)])

    router.get_backend('elasticsearch').index_manager.update()
    index3 = router.get_backend('elasticsearch').index_manager.get_index(
      'kronos')

    # Has index rolled over each time?
    self.assertTrue(index1 != index2)
    self.assertTrue(index2 != index3)
    self.assertTrue(index1 != index3)

    events = self.get('test_aliasing', time1, time3)
    self.assertEqual(len(events), 20)
    self.assertEqual(events, sorted(events,
                                    key=lambda e: TimeUUID(e[ID_FIELD])))

    events = self.get('test_aliasing', time1, time2)
    self.assertEqual(len(events), 15)
    self.assertEqual(events, sorted(events,
                                    key=lambda e: TimeUUID(e[ID_FIELD])))

    events = self.get('test_aliasing', time2, time3)
    self.assertEqual(len(events), 15)
    self.assertEqual(events, sorted(events,
                                    key=lambda e: TimeUUID(e[ID_FIELD])))

    es = router.get_backend('elasticsearch').es
    aliases = es.indices.get_aliases(index=[index1, index2, index3])
    self.assertEqual(len(aliases), 2)
    self.assertTrue(index1 in aliases)
    self.assertTrue(index2 in aliases)
    self.assertEqual(set(aliases[index1]['aliases']) &
                     set(aliases[index2]['aliases']),
                     set(['kronos_test:kronos:2014.01.01.01']))
    self.assertEqual(set(aliases[index1]['aliases']),
                     set(['kronos_test:kronos:2014.01.01.00',
                          'kronos_test:kronos:2014.01.01.01']))
    self.assertEqual(set(aliases[index2]['aliases']),
                     set(['kronos_test:kronos:2014.01.01.01',
                          'kronos_test:kronos:2014.01.01.02']))

    self.assertEqual(es.count(index=index1)['count'], 10)
    self.assertEqual(es.count(index=index2)['count'], 10)
    self.assertEqual(es.count(index=index3,
                              ignore_unavailable=True).get('count', 0), 0)

  def test_rollover(self):
    settings.storage.elasticsearch.rollover_size = 10
    settings.storage.elasticsearch.rollover_check_period_seconds = 2
    reload_router()
    
    indices = set()
    for i in xrange(50):
      indices.add(router.get_backend('elasticsearch')
                  .index_manager.get_index('kronos'))
      self.put('test_rollover', [{TIMESTAMP_FIELD: 0} for _ in xrange(5)])
      gevent.sleep(0.05)

    # Has index rolled over?
    self.assertTrue(len(indices) > 1)

    # No events were lost?
    events = self.get('test_rollover', 0, 1)
    self.assertEqual(len(events), 50 * 5)
    self.assertEqual(events, sorted(events,
                                    key=lambda e: TimeUUID(e[ID_FIELD])))

    es = router.get_backend('elasticsearch').es
    index = router.get_backend('elasticsearch').index_manager.get_index(
      'kronos')
    indices.discard(index)
    for index in indices:
      self.assertTrue(es.count(index=index)['count'] >= 10)

  def test_concurrent_rollover(self):
    settings.storage.elasticsearch.rollover_size = 10
    reload_router()

    index_manager1 = router.get_backend('elasticsearch').index_manager
    index_manager1.kill_rollover_worker()

    # Create another IndexManager. This should pull index names from the
    # previously created directory by index_manager1.
    index_manager2 = IndexManager(router.get_backend('elasticsearch'))
    index_manager2.kill_rollover_worker()

    index1 = index_manager1.get_index('kronos')
    self.assertEqual(index_manager1.get_index('kronos'),
                     index_manager2.get_index('kronos'))

    self.put('test_concurrent_rollover',
             [{TIMESTAMP_FIELD: 0} for _ in xrange(10)])

    # Update index_manager2. It should rollover.
    index_manager2.update()
    self.assertEqual(index1, index_manager1.get_index('kronos'))
    self.assertNotEqual(index_manager1.get_index('kronos'),
                        index_manager2.get_index('kronos'))

    # This should read updated index set by index_manager2.
    index_manager1.update()
    self.assertEqual(index_manager1.get_index('kronos'),
                     index_manager2.get_index('kronos'))
