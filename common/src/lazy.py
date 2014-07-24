import operator

from threading import Lock

_NONE = object()


def wrapped_obj_proxy(func):
  def proxy(self, *args, **kwargs):
    if self.__wrapped__ == _NONE:
      self.__setup__()
    return func(self.__wrapped__, *args, **kwargs)
  return proxy


class _LazyObject(object):
  def __init__(self, *args, **kwargs):
    self.__init_args__ = args
    self.__init_kw__ = kwargs
    self.__wrapped__ = _NONE
    self.__init_lock__ = Lock()

  def __setup__(self):
    # This lock is needed because calling the constructor of `__wrapped_cls__`
    # could be pre-empted (or cooperatively yields) before completion and we
    # could end up creating the object multiple times.
    with self.__init_lock__:
      if self.__wrapped__ != _NONE:
        return
      self.__wrapped__ = self.__wrapped_cls__(*self.__init_args__,
                                              **self.__init_kw__)
      del self.__init_args__
      del self.__init_kw__

  # Introspection support.
  __dir__ = wrapped_obj_proxy(dir)
  
  # Pretend to be an instance of the wrapped class; to make sure `isinstance`
  # related equality checks work.
  __class__ = property(lambda self: self.__wrapped_cls__)

  # Calling cmp on the wrapped object will automatically take care of using the
  # right object comparison function.
  __cmp__ = wrapped_obj_proxy(cmp)
  
  # Hashing support.
  __hash__ = wrapped_obj_proxy(hash)

  # Stringifying support.
  __str__ = wrapped_obj_proxy(str)
  __unicode__ = wrapped_obj_proxy(unicode)
  __repr__ = wrapped_obj_proxy(repr)
  
  # Dictionary methods support.
  __getitem__ = wrapped_obj_proxy(operator.getitem)
  __setitem__ = wrapped_obj_proxy(operator.setitem)
  __delitem__ = wrapped_obj_proxy(operator.delitem)

  # Iterable/list support.
  __len__ = wrapped_obj_proxy(len)
  __iter__ = wrapped_obj_proxy(iter)
  __contains__ = wrapped_obj_proxy(operator.contains)

  # Boolean support.
  __nonzero__ = wrapped_obj_proxy(bool)

  # Attribute getter/setter/deleter support.
  __getattr__ = wrapped_obj_proxy(getattr)
  
  def __setattr__(self, attr, value):
    if attr in ('__wrapped__', '__init_args__', '__init_kw__', '__init_lock__'):
      # Assign to __dict__ to avoid an infinite loop.
      self.__dict__[attr] = value
    else:
      if self.__wrapped__ == _NONE:
        self.__setup__()
      setattr(self.__wrapped__, attr, value)

  def __delattr__(self, attr):
    if attr == '__wrapped__':
      raise TypeError("Can't delete __wrapped__.")
    if attr in ('__init_args__', '__init_kw__'):
      self.__dict__.pop(attr)
      return
    if self.__wrapped__ == _NONE:
      self.__setup__()
    delattr(self.__wrapped__, attr)


class LazyObjectMetaclass(type):
  """
  This metaclass will will make any class instantiate instances *lazily*. What
  this means is that the __init__() function will be called lazily; for instance
  when some property is fetched etc.

  class Messenger(object):
    __metaclass__ = LazyObjectMetaclass
    
    def __init__(self, host):
      print 'Creating a new Messenger instance.'
      self.connection = connect(host)

    def message(self, msg):
      self.connection.send(msg)

  messenger = Messenger()
  # Nothing gets printed. The __init__() method hasn't been called yet.
  messenger.message('hello world')
  # 'Creating a new Messenger instance.' gets printed now.

  Funky note: str(Messenger) => <class 'kronos.common.lazy.LazyMessenger'>.
  See the __new__ function to figure out why.

  The original need arose from the fact that our StorageRouter implementation
  creates connections for all storage backends when its constructed. So
  any import like:
    > from kronos.storage.router import router
  would underneath the hood create connection objects to all storage backends.
  This was not playing well the pre-fork model of uWSGI.
  """
  def __new__(cls, name, bases, dict):
    # Create the class instance.
    wrapped_cls = super(LazyObjectMetaclass, cls).__new__(cls, name, bases,
                                                          dict)
    # Dynamically subclass _LazyObject to use `wrapped_cls` as its *base*
    # class. This is some crazy stuff. Basically the class that uses this
    # metaclass is swapped out dynamically with a the following custom
    # subclass of _LazyObject. This subclass *wraps* the class that originally
    # wanted to be instantiated lazily.
    return type('Lazy%s' % name, (_LazyObject, ),
                {'__wrapped_cls__': wrapped_cls})

