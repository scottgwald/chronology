from cassandra.cluster import Cluster
from cassandra.protocol import ConfigurationException

from benchmarks.cassandra import settings

def clean():
  cluster = Cluster(settings.storage['cassandra']['hosts'])
  session = cluster.connect()
  try:
    session.execute('DROP KEYSPACE kronos_benchmark_kronos', timeout=30)
  except ConfigurationException, e:
    if 'Cannot drop non existing keyspace' in e.message:
      return
    raise
