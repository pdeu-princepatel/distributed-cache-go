package cache

import "container/list"

type lruStack struct {
	list     *list.List               // Doubly linked list to track order (Front = Hot, Back = Cold)
	elements map[string]*list.Element // Map to find the list node instantly given a key
	capacity int                      // Maximum items allowed
}

func newLRU(capacity int) *lruStack {
	return &lruStack{
		list:     list.New(),
		elements: make(map[string]*list.Element),
		capacity: capacity,
	}
}

// promote moves an existing key to the "Hot" (Front) end
func (l *lruStack) promote(key string) {
	if el, ok := l.elements[key]; ok {
		l.list.MoveToFront(el)
	}
}

// add inserts a new key or promotes an existing one; returns evicted key if full
func (l *lruStack) add(key string) string {
	if el, ok := l.elements[key]; ok {
		l.list.MoveToFront(el)
		return ""
	}

	el := l.list.PushFront(key)
	l.elements[key] = el

	// If we exceeded capacity, trigger eviction
	if l.list.Len() > l.capacity {
		return l.evict()
	}
	return ""
}

// evict removes the oldest item (at the Back) and returns its key
func (l *lruStack) evict() string {
	el := l.list.Back()
	if el == nil {
		return ""
	}
	key := el.Value.(string)
	l.list.Remove(el)       // Remove from list
	delete(l.elements, key) // Remove from internal element tracker
	return key
}

func (l *lruStack) remove(key string) {
	if el, ok := l.elements[key]; ok {
		l.list.Remove(el)
		delete(l.elements, key)
	}
}
