import sys
sys.path.append("d:\Project\ssot")

from collections import OrderedDict, namedtuple

class LRUCache:

    def __init__(self, max_size: int):
        self.cache = OrderedDict
        self.max_size = max_size
        self.hits = 0
        self.misses = 0
    
    def get(self, key):
        if key not in self.cache:
            self.misses += 1
            return None
        else:
            self.hits += 1
            self.cache.move_to_end(key)
            return self.cache[key]
        
    
    def put(self, key, value) -> None:
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > len(self.max_size):
            self.cache.popitem(last=False)

    def get_cache_metrics(self):
        CacheMetrics = namedtuple('CacheMetrics', ['hits','misses','current_size', 'max_size'])

        return CacheMetrics(self.hits, self.misses, len(self.cache), self.max_size)