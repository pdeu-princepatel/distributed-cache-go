package cache

import "sync/atomic"

type Metrics struct {
	Hits      uint64 // Successful lookups
	Misses    uint64 // Key not found or expired
	Evictions uint64 // Items kicked out due to capacity or TTL
}

// Using atomic.Add ensures thread-safety even if multiple CPU cores update this simultaneously
func (m *Metrics) addHit()      { atomic.AddUint64(&m.Hits, 1) }
func (m *Metrics) addMiss()     { atomic.AddUint64(&m.Misses, 1) }
func (m *Metrics) addEviction() { atomic.AddUint64(&m.Evictions, 1) }
