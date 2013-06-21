class InMemoryLRUCache(object):
  """
  In-memory LRU Cache backed by a doubly linked circular list with a sentinel
  keeping track of the LRU and MRU entries. The keys are also stored in a dict
  to allow for O(1) look up and insert time.
  """

  def __init__(self, max_items=1000):
    self.max_items = max_items
    self.cache = dict() # key => [value, next_ptr, previous_ptr]
    self.sentinel = [None, None, None]


  def mark_as_lru(self, entry):
    value, next, previous = entry
    first = self.sentinel[2]
    first[1] = self.sentinel[2] = entry
    entry[2] = first
    entry[1] = self.sentinel  
    previous[1] = next
    next[2] = previous
  
  def set(self, key, value):
    entry = self.cache.get(key, self.sentinel)
    if entry == self.sentinel:
      if len(self.cache) == self.max_items:
        last_next, last_previous, last_value = self.sentinel[1]
        del self.cache[key], last_previous, last_value
        self.sentinel[1] = last_next
        last_next[2] = self.sentinel
      old_first = self.sentinel[2] or self.sentinel
      entry = [value, self.sentinel, old_first]
      old_first[1] = self.sentinel[2] = self.cache[key] = entry 
    else:
      self.mark_as_lru(entry)

  def get(self, key):
    entry = self.cache[key]
    self.mark_as_lru(entry)
    return entry[0]
