package cache

// code to run testing
// cd core
// go test -bench .
// go test -bench . -race

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkSet measures how fast we can write to the cache
func BenchmarkSet(b *testing.B) {
	c := New(10000, 0, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(fmt.Sprintf("key-%d", i), "value", time.Minute)
	}
}

// BenchmarkGet measures how fast we can read a "hot" key
func BenchmarkGet(b *testing.B) {
	c := New(10000, 0, nil)
	c.Set("foo", "bar", time.Minute)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("foo")
	}
}

// BenchmarkParallelGet simulates 100+ people hitting the cache at once
// This will reveal if our Global Mutex is a bottleneck
func BenchmarkParallelGet(b *testing.B) {
	c := New(10000, 0, nil)
	c.Set("foo", "bar", time.Minute)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get("foo")
		}
	})
}
