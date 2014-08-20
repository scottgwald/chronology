import unittest

from metis.core.execute.utils import get_property
from metis.core.execute.utils import get_properties_accessed_by_value
from metis.core.query.value import Add
from metis.core.query.value import Constant
from metis.core.query.value import Property


class QueryUtilsTestCase(unittest.TestCase):
  def test_get_property(self):
    self.assertEqual(get_property({'a': 1, 'b': 2}, 'a'), 1)
    self.assertEqual(get_property({'a.c': 1, 'b': 2}, 'a.c'), 1)
    self.assertEqual(get_property({'a': {'c': 1}, 'b': 2}, 'a.c'), 1)
    # We prefer shallow keys over deep ones.
    self.assertEqual(get_property({'a': {'c': 1}, 'a.c': 2, 'b': 3}, 'a.c'), 2)

  def test_get_properties_accessed_by_value(self):
    self.assertEqual(
      get_properties_accessed_by_value(Property('lolcat')),
      ['lolcat'])
    self.assertEqual(get_properties_accessed_by_value(Constant(0)),
                     [])
    self.assertEqual(
      get_properties_accessed_by_value(
        Add([Property('lolcat'),
             Constant(1),
             Add([Property('hello'), Constant(2)])])
        ),
      ['lolcat', 'hello'])
