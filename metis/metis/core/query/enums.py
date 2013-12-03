from metis.utils.enum import Enum

class AggregateType(Enum):
  MIN = 'min'
  MAX = 'max'
  AVG = 'avg'
  COUNT = 'count'
  SUM = 'sum'
  VALUE_COUNT = 'valuecount'
                    

class ConditionType(Enum):
  AND = 'and'
  OR = 'or'


class ConditionOpType(Enum):
  LT = 'lt'
  LTE = 'lte'
  GT = 'gt'
  GTE ='gte'
  EQ = 'eq'
  CONTAINS = 'contains'
  IN = 'in'
  REGEX = 'regex'


class FunctionType(Enum):
  CEIL = 'ceil'
  FLOOR = 'floor'
  LOWERCASE = 'lowercase'
  UPPERCASE = 'uppercase'
  RANDOM_INT = 'randint'
  ADD = 'add'
  SUBTRACT = 'subtract'
  

class OperatorType(Enum):
  KRONOS = 'kronos'
  PROJECT = 'project'
  FILTER = 'filter'
  ORDER_BY = 'orderby'
  LIMIT = 'limit'
  AGGREGATE = 'aggregate'
  JOIN = 'join'


class ValueType(Enum):
  CONSTANT = 'constant'
  FUNCTION = 'function'
  PROPERTY = 'property'
