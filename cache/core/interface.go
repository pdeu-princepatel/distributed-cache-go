package cache

import "time"

// Cacher allows us to mock the cache
type Cacher interface {
	Set(key string, value interface{}, ttl time.Duration)
	Get(key string) (interface{}, bool)
	Delete(key string)
	Metrics() (hits, misses, evictions uint64)
}

// Ensure our Cache struct fits the interface
var _ Cacher = (*Cache)(nil)

// Add these to satisfy the interface
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeItem(key)
}

func (c *Cache) Metrics() (uint64, uint64, uint64) {
	return c.metrics.Hits, c.metrics.Misses, c.metrics.Evictions
}
