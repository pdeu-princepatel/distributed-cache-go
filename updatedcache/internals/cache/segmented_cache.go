package cache

/*
SEGMENTED CACHE LOGIC:

We divide cache into two layers:

1️ Warm Segment
   - All new keys go here first.
   - Acts as a probation area.

2️ Hot Segment
   - Frequently accessed keys get promoted here.
   - Protected from eviction.

Promotion Strategy:
- Each access increases frequency.
- Once frequency crosses threshold,
  promotion happens probabilistically.
- This prevents hot pollution from temporary spikes.

Goal:
Avoid promoting keys too early.
*/
/*
IMPROVED SEGMENTED CACHE LOGIC

Problem:
Frequency grows forever:
- Integer overflow risk
- Probability becomes always 1
- Frequency loses meaning

Solution:
1️ Cap frequency at maxFrequency
2️ Reset frequency when promoted to Hot
3️	 Keep probability bounded and meaningful

This keeps promotion behavior stable long-term.
*/

/*
SEGMENTED CACHE WITH PROPER EVICTION PROPAGATION

Problem:
When promoting from Warm → Hot:
    c.hot.put(e)

If Hot is full:
    hot.put() evicts LRU item

But we were ignoring returned evicted entry.

This causes:
- Silent data loss
- No metrics
- No TTL cleanup
- No observability

Solution:
1️ Capture returned evicted entry
2️ Forward to eviction handler
3️Keep system extensible for metrics / TTL / logging

SEGMENTED CACHE + SINGLEFLIGHT INTEGRATION

Flow:

1️ Try cache (Hot → Warm)
2️ If miss:
      use singleflight.Do()
      only ONE goroutine loads from DB
3️ Insert loaded value into cache
4️ Return shared result to all waiting goroutines

This prevents cache stampede.
*/

import (
	"context"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

const maxFrequency = 10

type LoaderFunc func(ctx context.Context, key string) (interface{}, error)

// EvictionCallback allows external handling
type EvictionCallback func(key string, value interface{})

// SegmentedCache represents Hot + Warm cache
type SegmentedCache struct {
	mu sync.RWMutex

	hot  *segment
	warm *segment

	promotionThreshold int
	rand               *rand.Rand

	onEvict    EvictionCallback // optional eviction observer
	sf         *SingleFlight    //integrated singleflight
	defaultTTL time.Duration    // default ttl
	softTTL    time.Duration    // soft ttl
	metrics    *Metrics
}

// sharding
type ShardedCache struct {
	shards      []*SegmentedCache
	shardMask   uint64
	stopJanitor chan struct{}
}

// Constructor with optional eviction callback

func NewSegmentedCache(hotCap int, warmCap int, threshold int, ttl time.Duration, cb EvictionCallback) *SegmentedCache {
	soft := ttl * 7 / 10 // 70 % of hard ttl
	return &SegmentedCache{
		hot:                newSegment(hotCap),
		warm:               newSegment(warmCap),
		promotionThreshold: threshold,
		rand:               rand.New(rand.NewSource(time.Now().UnixNano())),
		onEvict:            cb,
		sf:                 NewSingleFlight(), // initialize single flight
		defaultTTL:         ttl,
		softTTL:            soft,
		metrics:            &Metrics{},
	}
}

// constructor for sharding
func NewShardedCache(
	shardCount int,
	hotCap int,
	warmCap int,
	threshold int,
	cb EvictionCallback,
	ttl time.Duration,
) *ShardedCache {

	// shardCount must be power of 2
	if shardCount&(shardCount-1) != 0 {
		panic("shardCount must be power of 2")
	}

	shards := make([]*SegmentedCache, shardCount)

	for i := 0; i < shardCount; i++ {
		shards[i] = NewSegmentedCache(hotCap, warmCap, threshold, ttl, cb)
	}

	return &ShardedCache{
		shards:      shards,
		shardMask:   uint64(shardCount - 1),
		stopJanitor: make(chan struct{}),
	}
}

// hashing function
func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

// select shard
func (sc *ShardedCache) getShard(key string) *SegmentedCache {
	hash := hashKey(key)
	index := hash & sc.shardMask
	return sc.shards[index]
}

// shard entry point
func (sc *ShardedCache) GetWithLoad(
	ctx context.Context,
	key string,
	loader func(context.Context, string) (interface{}, error),
) (interface{}, error) {

	// Select shard based on key hash
	shard := sc.getShard(key)

	// Delegate operation to that shard
	return shard.GetWithLoad(ctx, key, loader)
}

// GetWithLoad attempts cache, otherwise loads via singleflight
func (c *SegmentedCache) GetWithLoad(
	ctx context.Context,
	key string,
	loader func(context.Context, string) (interface{}, error),
) (interface{}, error) {

	// ---------- FAST PATH ----------
	if val, ok := c.getfromcache(key); ok {

		c.mu.RLock()

		var e *entry

		if v, ok := c.hot.get(key); ok {
			e = v
		} else if v, ok := c.warm.get(key); ok {
			e = v
		}

		now := time.Now()

		// soft ttl expired but hard ttl still valid
		if e != nil && now.After(e.refreshAt) && now.Before(e.expireAt) {

			c.mu.RUnlock()
			c.mu.Lock()

			if !e.refreshing {

				e.refreshing = true

				go c.refreshAsync(key, loader)
			}

			c.mu.Unlock()
			c.mu.RLock()
		}

		c.mu.RUnlock()

		return val, nil
	}

	// ---------- MISS ----------
	c.metrics.addMiss()

	ch := c.sf.DoChan(key, func() (interface{}, error) {

		if val, ok := c.getfromcache(key); ok {
			return val, nil
		}

		c.metrics.addLoad()

		loaderCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		return loader(loaderCtx, key)
	})

	select {

	case <-ctx.Done():
		return nil, ctx.Err()

	case res := <-ch:

		if res.Err != nil {
			c.metrics.addLoadError()
			return nil, res.Err
		}

		c.Put(key, res.Val)

		return res.Val, nil
	}
}

// Get retrieves value from Hot or Warm segments
// Uses read lock first for fast path, then upgrades to write lock if mutation is needed
func (c *SegmentedCache) getfromcache(key string) (interface{}, bool) {

	now := time.Now()

	// ---------- READ PHASE ----------
	c.mu.RLock()

	e, ok := c.hot.items[key]
	if ok {
		ent := e.Value.(*entry)

		val := ent.value
		expire := ent.expireAt

		c.mu.RUnlock()

		// ---------- EXPIRE CHECK ----------
		if now.After(expire) {

			c.mu.Lock()
			if ele, ok := c.hot.items[key]; ok {
				e := ele.Value.(*entry)
				c.hot.remove(key)
				c.handleEviction(e)
			}
			c.mu.Unlock()

			return nil, false
		}

		// ---------- WRITE PHASE ----------
		c.mu.Lock()

		// re-fetch to avoid stale pointer
		if ele, ok := c.hot.items[key]; ok {
			e := ele.Value.(*entry)

			if e.frequency < maxFrequency {
				e.frequency++
			}

			c.hot.ll.MoveToFront(ele)
			c.metrics.addHit()
		}

		c.mu.Unlock()

		return val, true
	}

	// ---------- WARM ----------
	e, ok = c.warm.items[key]
	if ok {
		ent := e.Value.(*entry)

		val := ent.value
		expire := ent.expireAt
		freq := ent.frequency

		c.mu.RUnlock()

		if now.After(expire) {

			c.mu.Lock()
			if ele, ok := c.warm.items[key]; ok {
				e := ele.Value.(*entry)
				c.warm.remove(key)
				c.handleEviction(e)
			}
			c.mu.Unlock()

			return nil, false
		}

		c.mu.Lock()

		// re-check existence
		if ele, ok := c.warm.items[key]; ok {

			e := ele.Value.(*entry)

			if e.frequency < maxFrequency {
				e.frequency++
			}

			// promotion
			if c.shouldPromote(freq) {

				c.warm.remove(key)

				e.frequency = 1

				evicted := c.hot.put(e)
				if evicted != nil {
					c.handleEviction(evicted)
				}
			} else {
				c.warm.ll.MoveToFront(ele)
			}

			c.metrics.addHit()
		}

		c.mu.Unlock()

		return val, true
	}

	c.mu.RUnlock()
	return nil, false
}

// Put inserts into warm
func (c *SegmentedCache) Put(key string, value interface{}) {

	c.mu.Lock()
	defer c.mu.Unlock()

	e := &entry{
		key:       key,
		value:     value,
		frequency: 1,
		expireAt:  time.Now().Add(c.defaultTTL),
		refreshAt: time.Now().Add(c.softTTL),
	}

	evicted := c.warm.put(e)

	// Handle eviction from warm
	if evicted != nil {
		c.handleEviction(evicted)
	}
}

// shouldPromote calculates probability
func (c *SegmentedCache) shouldPromote(freq int) bool {

	if freq < c.promotionThreshold {
		return false
	}

	prob := float64(freq) / float64(maxFrequency)

	if prob > 1.0 {
		prob = 1.0
	}

	return c.rand.Float64() < prob
}

// handleEviction centralizes eviction handling
func (c *SegmentedCache) handleEviction(e *entry) {

	c.metrics.addEviction()
	// Call external eviction callback if provided
	if c.onEvict != nil {
		c.onEvict(e.key, e.value)
	}

	// Future safe extension:
	// - metrics++
	// - TTL cleanup
	// - logging
}

// stats exposed for cache
func (c *SegmentedCache) Stats() Metrics {
	return c.metrics.Snapshot()
}

// stats exposed for cache
func (sc *ShardedCache) Stats() Metrics {

	var total Metrics

	for _, shard := range sc.shards {
		m := shard.Stats()

		total.Hits += m.Hits
		total.Misses += m.Misses
		total.Evictions += m.Evictions
		total.Loads += m.Loads
		total.LoadErrors += m.LoadErrors
	}

	return total
}

// cleanExpired scans both segments and removes expired entries
func (c *SegmentedCache) cleanExpired() {

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// -------- Scan HOT segment --------
	for key, elem := range c.hot.items {

		// Extract actual entry stored in list element
		e := elem.Value.(*entry)

		if now.After(e.expireAt) {

			c.hot.remove(key)

			c.handleEviction(e)
		}
	}

	// -------- Scan WARM segment --------
	for key, elem := range c.warm.items {

		e := elem.Value.(*entry)

		if now.After(e.expireAt) {

			c.warm.remove(key)

			c.handleEviction(e)
		}
	}
}

// Delete removes a key from cache
func (sc *ShardedCache) Delete(key string) {

	shard := sc.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if e, ok := shard.hot.items[key]; ok {
		shard.hot.remove(key)
		shard.handleEviction(e.Value.(*entry))
		return
	}

	if e, ok := shard.warm.items[key]; ok {
		shard.warm.remove(key)
		shard.handleEviction(e.Value.(*entry))
	}
}

// Async refreshing
func (c *SegmentedCache) refreshAsync(
	key string,
	loader LoaderFunc,
) {

	resCh := c.sf.DoChan("refresh:"+key, func() (interface{}, error) {

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		return loader(ctx, key)
	})

	select {

	case res := <-resCh:

		c.mu.Lock()
		defer c.mu.Unlock()

		var e *entry

		if v, ok := c.hot.get(key); ok {
			e = v
		} else if v, ok := c.warm.get(key); ok {
			e = v
		}

		if e == nil || e.key != key {
			return
		}

		e.refreshing = false

		if res.Err != nil {
			return
		}

		now := time.Now()

		e.value = res.Val
		e.expireAt = now.Add(c.defaultTTL)
		e.refreshAt = now.Add(c.softTTL)

	case <-time.After(5 * time.Second):

		// prevent stuck refreshing state
		c.mu.Lock()

		if v, ok := c.hot.get(key); ok {
			v.refreshing = false
		} else if v, ok := c.warm.get(key); ok {
			v.refreshing = false
		}

		c.mu.Unlock()
	}
}

func (sc *ShardedCache) Set(key string, value interface{}) {
	shard := sc.getShard(key)
	shard.Put(key, value)
}
func (sc *ShardedCache) Exists(key string) bool {
	shard := sc.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if _, ok := shard.hot.items[key]; ok {
		return true
	}
	if _, ok := shard.warm.items[key]; ok {
		return true
	}
	return false
}
func (sc *ShardedCache) Get(key string) (interface{}, bool) {
	shard := sc.getShard(key)
	return shard.getfromcache(key)
}
