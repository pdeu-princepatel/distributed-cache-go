package cache

import "time"

// startJanitor runs in a separate goroutine as long as the app lives
func (c *Cache) startJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		c.DeleteExpired() // Periodically trigger cleanup
	}
}

func (c *Cache) DeleteExpired() {
	c.mu.Lock() // Stop all other operations while we scan
	defer c.mu.Unlock()

	// Full scan of the map (Note: can be slow if map is massive)
	for key, item := range c.items {
		if item.Expired() {
			c.removeItem(key)
		}
	}
}
