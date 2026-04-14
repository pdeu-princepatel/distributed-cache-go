package cache

/*
SEGMENT LOGIC:

Each segment represents one LRU cache layer (Hot or Warm).

It maintains:
- A doubly linked list (to track LRU order)
- A map for O(1) access
- A capacity limit

When capacity exceeds:
- The least recently used item (back of list) is evicted.

This is the building block for our segmented cache.
*/

import (
	"container/list"
	"time"
)

// entry represents a cache item.
type entry struct {
	key       string      // cache key
	value     interface{} // stored value
	frequency int         // number of accesses (used for promotion decision)
	expireAt  time.Time   // hard ttl
	refreshAt time.Time   // soft ttl

	refreshing bool
}

// segment represents one LRU layer (Hot or Warm).
type segment struct {
	capacity int                      // max allowed items
	ll       *list.List               // doubly linked list (LRU order)
	items    map[string]*list.Element // map for O(1) lookup
}

// newSegment initializes a new LRU segment.
func newSegment(cap int) *segment {
	return &segment{
		capacity: cap,                            // set max capacity
		ll:       list.New(),                     // create empty linked list
		items:    make(map[string]*list.Element), // create lookup map
	}
}

// get retrieves an entry and moves it to front (most recently used).
func (s *segment) get(key string) (*entry, bool) {

	// Check if key exists in map
	if ele, ok := s.items[key]; ok {

		// Move accessed item to front (MRU position)
		s.ll.MoveToFront(ele)

		// Return the actual entry
		return ele.Value.(*entry), true
	}

	return nil, false
}

// put inserts or updates an entry.
func (s *segment) put(e *entry) (evicted *entry) {

	// If key already exists, update value and move to front
	if ele, ok := s.items[e.key]; ok {
		s.ll.MoveToFront(ele)
		ele.Value = e
		return nil
	}

	// Insert new element at front (MRU)
	ele := s.ll.PushFront(e)
	s.items[e.key] = ele

	// If capacity exceeded, remove LRU (back)
	if s.ll.Len() > s.capacity {

		last := s.ll.Back()
		if last != nil {
			s.ll.Remove(last)
			ent := last.Value.(*entry)
			delete(s.items, ent.key)
			return ent // return evicted item
		}
	}
	return nil
}

// remove deletes a key from segment.
func (s *segment) remove(key string) {

	if ele, ok := s.items[key]; ok {
		s.ll.Remove(ele)
		delete(s.items, key)
	}
}
