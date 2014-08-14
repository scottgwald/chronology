import unittest

from metis.core.execute.utils import get_property
from metis.core.execute.utils import get_property_names_from_getter
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
  
  def test_get_property_names_from_getter(self):
    self.assertEqual(get_property_names_from_getter(Property('lolcat').to_dict()),
                     ['lolcat'])
    self.assertEqual(get_property_names_from_getter(Constant(0).to_dict()),
                     [])
    self.assertEqual(
      get_property_names_from_getter(
        Add([Property('lolcat'),
             Constant(1),
             Add([Property('hello'), Constant(2)])]
            ).to_dict()
        ),
      ['lolcat', 'hello'])
