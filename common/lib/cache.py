import functools


class InMemoryLRUCache(object):
  """
  In-memory LRU Cache backed by a doubly linked circular list with a sentinel
  keeping track of the LRU and MRU entries. The keys are also stored in a dict
  to allow for O(1) look up and insert time.
  """

  def __init__(self, max_items=1000):
    self.max_items = max_items
    self.cache = {} # key => [value, next_ptr, prev_ptr]
    # sentinel comes AFTER the head and BEFORE the tail. head is the MRU cache
    # entry.
    self.sentinel = [None] * 3

  def mark_as_lru(self, entry):
    _, entry_next, entry_previous = entry
    head = self.sentinel[2]
    head[1] = self.sentinel[2] = entry # head.next = sentinel.prev = entry
    entry[2] = head # entry.previous = head
    entry[1] = self.sentinel  # entry.next = sentinel
    entry_previous[1] = entry_next # entry_previous.next = entry_next
    entry_next[2] = entry_previous # entry_next.previous = entry_previous
    
  def set(self, key, value):
    entry = self.cache.get(key, self.sentinel)
    if entry == self.sentinel:
      if len(self.cache) == self.max_items:
        _, tail_next, tail_previous = self.sentinel[1]
        self.sentinel[1] = tail_next # sentinel.next = tail.next
        tail_next[2] = self.sentinel # tail.next.previous = sentinel
      head = self.sentinel[2] or self.sentinel
      entry = [value, self.sentinel, head]
      head[1] = self.sentinel[2] = self.cache[key] = entry 
    else:
      entry[0] = value
      self.mark_as_lru(entry)

  def get(self, key):
    entry = self.cache[key]      
    self.mark_as_lru(entry)
    return entry[0]

  def delete(self, key):
    entry = self.cache.pop(key)
    entry[2][1] = entry[1] # entry.prev.next = entry.next
    entry[1][2] = entry[2] # entry.next.prev = entry.prev
    return entry[0]

  def clear(self):
    self.cache.clear()
    self.sentinel[1] = self.sentinel[2] = None


def memoize(max_items=1000):
  def decorator(func):
    cache = InMemoryLRUCache(max_items=max_items)
    @functools.wraps(func)
    def wrapper(*args):
      try:
        success, value = cache.get(args)
      except KeyError:
        try:
          value = func(*args)
          success = True
        except Exception, e:
          value = e
          success = False
        cache.set(args, (success, value))
      if success:
        return value
      raise value
    return wrapper
  return decorator
