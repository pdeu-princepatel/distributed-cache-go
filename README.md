# Distributed Cache in Go — Part 1 (Core Foundation)

This project is part of a series where I’m building a **distributed cache system from scratch in Go**, focusing on real-world system design, concurrency, and performance.

This first phase focuses on building a **production-style in-memory cache foundation**.

---

## Features (Current)

- Thread-safe in-memory cache
- O(1) read/write operations
- LRU (Least Recently Used) eviction
- TTL (Time-To-Live) based expiration
- Clean and modular design

---

## Design Overview

The cache is built using:

- **HashMap** → O(1) lookups
- **Doubly Linked List** → maintains LRU order
- **RWMutex** → ensures thread safety with optimized read/write locking

### Flow:

Set → Insert into map + move to front of LRU  
Get → Lookup from map + move to front  
Evict → Remove least recently used (tail of list)  
TTL → Expire entries based on time  

---

##  Project Structure

cache/  
 ├── cache.go      // Main cache interface  
 ├── store.go      // Core storage logic  
 ├── lru.go        // LRU implementation  
 ├── ttl.go        // Expiration logic  

---

##  Example Usage

```go
cache := NewCache(100) // capacity

cache.Set("key", "value", 5*time.Second)

val, err := cache.Get("key")
if err != nil {
    // handle miss / expiration
}

fmt.Println(val)
```

---

##  Key Learnings

- Thread safety is critical when handling concurrent access
- Simple designs can become bottlenecks under load
- LRU is effective but not always optimal for real-world patterns
- Expiration logic needs careful handling to avoid stale data

---

##  What’s Next

In the next phase, the cache will be optimized for high concurrency:

- Sharded cache (reduce lock contention)
- Segmented LRU (Hot/Warm layers)
- Soft TTL with async refresh
- Singleflight for request deduplication

---

##  Goal

This project is being built as a **learning + engineering exercise** to understand how real caching systems behave under load, rather than just implementing theoretical concepts.

---

## Contributions / Feedback

Open to suggestions, improvements, and discussions around design decisions.

---

##  Author

Built as part of my journey into **distributed systems and backend engineering using Go**.

By~ Prince Patel
