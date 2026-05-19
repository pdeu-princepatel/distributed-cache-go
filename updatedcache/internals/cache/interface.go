package cache

import "context"

// Cacher allows swapping cache implementation later
// (ex: Redis, Memcached, etc.)
type Cacher interface {

	// Fetch value from cache or load using loader
	GetWithLoad(ctx context.Context, key string, loader func(context.Context, string) (interface{}, error)) (interface{}, error)
	Delete(key string)
	// Return cache statistics
	Stats() Metrics
}

// Compile-time interface check
// Ensures ShardedCache implements Cacher
var _ Cacher = (*ShardedCache)(nil)
