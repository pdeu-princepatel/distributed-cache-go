package cache

/*
SINGLEFLIGHT LOGIC

Problem:
If many goroutines request the same key simultaneously:
- All miss cache
- All hit DB
- DB overload

Solution:
Singleflight ensures:
- Only ONE goroutine loads the key
- Others wait
- All receive same result

How it works:
1️ First caller creates a "call"
2️ Other callers join that call
3️ First caller executes load
4️ Result is shared to all waiting callers
*/

import (
	"fmt"
	"sync"
)

// handle all types of inputs in a single call
type Result struct {
	Val interface{}
	Err error
}

// call represents an in-flight request for a key
type call struct {
	result chan Result
}

// SingleFlight manages in-flight calls
type SingleFlight struct {
	mu sync.Mutex       // protects map
	m  map[string]*call // key → in-flight call
}

// NewSingleFlight creates instance
func NewSingleFlight() *SingleFlight {
	return &SingleFlight{
		m: make(map[string]*call),
	}
}

// Do executes loader function only once per key
func (sf *SingleFlight) DoChan(
	key string,
	loader func() (interface{}, error),
) <-chan Result {

	sf.mu.Lock()

	// If call already in progress for this key
	if c, ok := sf.m[key]; ok {
		sf.mu.Unlock()
		// Return same result
		return c.result
	}

	// Create new call
	c := &call{
		result: make(chan Result, 1),
	}
	// Store call in map
	sf.m[key] = c

	sf.mu.Unlock()

	// Execute loader gorutine
	go func() {
		// recovery logic
		defer func() {
			if r := recover(); r != nil {
				c.result <- Result{
					Val: nil,
					Err: fmt.Errorf("loader panic: %v", r),
				}
			}
			// always close channel
			close(c.result)

			// always remove call from map
			sf.mu.Lock()
			delete(sf.m, key)
			sf.mu.Unlock()

		}()
		val, err := loader()
		c.result <- Result{Val: val, Err: err}
	}()

	return c.result
}
