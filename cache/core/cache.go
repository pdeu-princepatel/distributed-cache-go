package cache

import (
	"sync"
	"time"
)

type Cache struct {
	mu        sync.RWMutex     // Protects the entire cache; allows multiple readers OR one writer
	items     map[string]*Item // The "Value Store"  for O(1) lookups
	lru       *lruStack        // The "Strategy" to track what to delete when full
	metrics   *Metrics         // The "Dashboard" for  performance tracking
	onEvicted EvictionCallBack // The "Event Hook" 	for custom cleanup logic
}

func New(capacity int, cleanupInterval time.Duration, callback EvictionCallBack) *Cache {
	c := &Cache{
		items:     make(map[string]*Item),
		lru:       newLRU(capacity),
		metrics:   &Metrics{},
		onEvicted: callback,
	}
	// If an interval is provided,sTart up the background cleaner (Janitor)
	if cleanupInterval > 0 {
		go c.startJanitor(cleanupInterval)
	}
	return c
}

func (c *Cache) Set(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock() // Full lock required because we are modifying the map and LRU list
	defer c.mu.Unlock()

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl) // Calculate absolute expiry time
	}

	item := &Item{Key: key, Value: value, Expiration: exp}
	c.items[key] = item

	// Add to LRU; if add() returns a key, it means the cache was full and evicted something
	if evictedKey := c.lru.add(key); evictedKey != "" {
		c.removeItem(evictedKey) // Delete evicted key from the actual map
		c.metrics.addEviction()
	}
}

func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.Lock() // Locked (not RLock) because promotion moves elements in the LRU linked-list
	defer c.mu.Unlock()

	item, found := c.items[key]
	if !found {
		c.metrics.addMiss()
		return nil, false
	}

	// Lazy Expiration: check if it's dead before returning it
	if item.Expired() {
		c.removeItem(key)
		c.metrics.addMiss()
		return nil, false
	}

	c.lru.promote(key) // Move to the front of the "recently used" list
	c.metrics.addHit()
	return item.Value, true
}

// removeItem is an internal helper. It assumes the caller already holds the lock.
func (c *Cache) removeItem(key string) {
	item, ok := c.items[key]
	if !ok {
		return
	}
	delete(c.items, key) // Remove from map
	c.lru.remove(key)    // Remove from LRU tracking
	if c.onEvicted != nil {
		c.onEvicted(key, item.Value) // Trigger user-defined callback
	}
}
