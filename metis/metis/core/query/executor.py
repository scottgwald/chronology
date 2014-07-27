from metis import app
from metis.core.query.operator import Operator

_SPARK_CONTEXT = None


def get_spark_context():
  from pyspark import SparkContext
  global _SPARK_CONTEXT

  if _SPARK_CONTEXT is None:
    # Also ship the Metis lib file so worker nodes can deserialize Metis
    # internal data structures.
    _SPARK_CONTEXT = SparkContext(app.config['SPARK_MASTER'],
                                  'Metis',
                                  pyFiles=[app.config['METIS_LIB_FILE']])
  return _SPARK_CONTEXT


def execute_compute_task(plan):
  spark_context = get_spark_context()
  return Operator.parse(plan).get_rdd(spark_context).collect()
