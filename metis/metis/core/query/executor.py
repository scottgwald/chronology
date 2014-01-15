from metis import app
from metis.core.query.operator import Operator

_SPARK_CONTEXT = None


def get_spark_context():
  from pyspark import SparkContext
  global _SPARK_CONTEXT
  
  if _SPARK_CONTEXT is None:
    _SPARK_CONTEXT = SparkContext(app.config['SPARK_MASTER'],
                                  'Metis',
                                  pyFiles=[app.config['METIS_LIB_FILE']])
  return _SPARK_CONTEXT


def execute_compute_task(plan):
  # Also ship the metis zip file so worker nodes can deserialize metis
  # internal objects.
  spark_context = get_spark_context()
  return Operator.parse(plan).get_rdd(spark_context).collect()
