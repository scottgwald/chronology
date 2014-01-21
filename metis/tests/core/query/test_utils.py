import unittest

from metis.core.query.enums import FunctionType
from metis.core.query.primitives import (p,
                                         f,
                                         c)
from metis.core.query.utils import (get_property,
                                    get_property_names_from_getter)


class QueryUtilsTestCase(unittest.TestCase):
  def test_get_property(self):
    self.assertEqual(get_property({'a': 1, 'b': 2}, 'a'), 1)
    self.assertEqual(get_property({'a.c': 1, 'b': 2}, 'a.c'), 1)
    self.assertEqual(get_property({'a': {'c': 1}, 'b': 2}, 'a.c'), 1)
    # We prefer shallow keys over deep ones.
    self.assertEqual(get_property({'a': {'c': 1}, 'a.c': 2, 'b': 3}, 'a.c'), 2)
  
  def test_get_property_names_from_getter(self):
    self.assertEqual(get_property_names_from_getter(p('lolcat')),
                     ['lolcat'])
    self.assertEqual(get_property_names_from_getter(c(0)),
                     [])
    self.assertEqual(
      get_property_names_from_getter(
        f(FunctionType.ADD,
          [p('lolcat'),
           c(1),
           f(FunctionType.ADD,
             [p('hello'),
              c(2)])]
          )
        ),
      ['lolcat', 'hello'])
