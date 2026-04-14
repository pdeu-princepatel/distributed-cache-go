package cache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func benchmarkLoader(ctx context.Context, key string) (interface{}, error) {
	time.Sleep(1 * time.Millisecond) // simulate DB call
	return "value-" + key, nil
}
func BenchmarkCacheRead(b *testing.B) {

	cache := NewShardedCache(
		16,   // shards
		1000, // hot capacity
		2000, // warm capacity
		3,    // promotion threshold
		nil,  // eviction callback
		time.Minute,
	)

	ctx := context.Background()

	// Preload cache
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		cache.GetWithLoad(ctx, key, benchmarkLoader)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		key := fmt.Sprintf("key-%d", i%1000)

		cache.GetWithLoad(ctx, key, benchmarkLoader)
	}
}

// it tests:
// cache hits
// LRU movement
// promotion logic
func BenchmarkCacheMiss(b *testing.B) {

	cache := NewShardedCache(
		16,
		1000,
		2000,
		3,
		nil,
		time.Minute,
	)

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		key := fmt.Sprintf("miss-%d", i)

		cache.GetWithLoad(ctx, key, benchmarkLoader)
	}
}

// it tests:
// singleflight behavior
// loader cost
// insert cost
func BenchmarkParallelCache(b *testing.B) {

	cache := NewShardedCache(
		32,
		5000,
		10000,
		3,
		nil,
		time.Minute,
	)

	ctx := context.Background()

	b.RunParallel(func(pb *testing.PB) {

		for pb.Next() {

			cache.GetWithLoad(ctx, "hot-key", benchmarkLoader)

		}
	})
}

// it tests:
// lock contention
// sharding effectiveness
// singleflight behavior

// singleflight benchmarking
func BenchmarkStampede(b *testing.B) {

	cache := NewShardedCache(16, 1000, 2000, 3, nil, time.Minute)

	ctx := context.Background()

	b.RunParallel(func(pb *testing.PB) {

		for pb.Next() {

			cache.GetWithLoad(ctx, "same-key", benchmarkLoader)

		}
	})
}

// Output of go test -bench .
// BenchmarkCacheRead-16            5265566               230.5 ns/op
// BenchmarkCacheMiss-16                768           1591247 ns/op
// BenchmarkParallelCache-16        8681182               137.6 ns/op
// BenchmarkStampede-16            10643914               115.9 ns/op
// PASS
// ok      updatecache     15.078s
