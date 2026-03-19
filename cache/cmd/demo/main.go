package main

// code to run demo
// go run ./cmd/demo

import (
	cache "cache/core" // Use the name from your go.mod
	"fmt"
	"time"
)

func main() {
	c := cache.New(10000, 1*time.Minute, nil)

	//Benchmark
	start := time.Now()
	iterations := 1000000

	fmt.Printf("Running %d operations...\n", iterations)

	for i := 0; i < iterations; i++ {
		c.Set(fmt.Sprintf("key-%d", i), i, time.Minute)
	}

	duration := time.Since(start)
	opPerSec := float64(iterations) / duration.Seconds()

	fmt.Printf("Finished in: %v\n", duration)
	fmt.Printf("Speed: %.2f ops/sec\n", opPerSec)

	// Check Metrics
	h, m, e := c.Metrics()
	fmt.Printf("Stats - Hits: %d, Misses: %d, Evictions: %d\n", h, m, e)
}

// on running this single core no distributed
