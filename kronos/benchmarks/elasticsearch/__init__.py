from elasticsearch import Elasticsearch

from benchmarks.elasticsearch import settings

def clean():
  es = Elasticsearch(hosts=settings.storage['elasticsearch']['hosts'])
  es.indices.delete([
    index
    for index in es.indices.status(index='_all')['indices']
    if index.startswith(settings.storage['elasticsearch']['index_prefix'])])
