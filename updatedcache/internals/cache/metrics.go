package cache

import "sync/atomic"

type Metrics struct {
	Hits       uint64 // Successful lookups
	Misses     uint64 // Key not found or expired
	Evictions  uint64 // Items kicked out due to capacity or TTL
	Loads      uint64 // loaders calls count
	LoadErrors uint64 // errors count during loader
}

//  read api
func (m *Metrics) Snapshot() Metrics {
	return Metrics{
		Hits:       atomic.LoadUint64(&m.Hits),
		Misses:     atomic.LoadUint64(&m.Misses),
		Evictions:  atomic.LoadUint64(&m.Evictions),
		Loads:      atomic.LoadUint64(&m.Loads),
		LoadErrors: atomic.LoadUint64(&m.LoadErrors),
	}
}

// Using atomic.Add ensures thread-safety even if multiple CPU cores update this simultaneously
func (m *Metrics) addHit()       { atomic.AddUint64(&m.Hits, 1) }
func (m *Metrics) addMiss()      { atomic.AddUint64(&m.Misses, 1) }
func (m *Metrics) addEviction()  { atomic.AddUint64(&m.Evictions, 1) }
func (m *Metrics) addLoad()      { atomic.AddUint64(&m.Loads, 1) }
func (m *Metrics) addLoadError() { atomic.AddUint64(&m.LoadErrors, 1) }
