package cache

import "time"

type EvictionCallBack func(key string, value interface{})

type Item struct {
	Key        string
	Value      interface{} // Flexible storage for any data type
	Expiration time.Time   // Absolute deadline
}

func (i *Item) Expired() bool {
	if i.Expiration.IsZero() {
		return false // No TTL set, item lives "forever" (until LRU eviction)
	}
	return time.Now().After(i.Expiration) // Returns true if current time is past expiration
}
