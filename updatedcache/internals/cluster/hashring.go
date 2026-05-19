package cluster

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// hashring structure
type HashRing struct {
	replicas int
	keys     []uint32
	hashMap  map[uint32]string
	mu       sync.RWMutex
}

// constructor for hashring
func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		replicas: replicas,
		keys:     make([]uint32, 0),
		hashMap:  make(map[uint32]string),
	}
}

// actual hash function
func (h *HashRing) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// add nodes with there virtual nodes
func (h *HashRing) AddNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.replicas; i++ {
		virtualnode := node + "-" + strconv.Itoa(i)
		hash := h.hash(virtualnode)
		h.keys = append(h.keys, hash)
		h.hashMap[hash] = node
	}
	// for binary search this is needed
	sort.Slice(h.keys, func(x, y int) bool { return h.keys[x] < h.keys[y] })
}

// remove node
func (h *HashRing) RemoveNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Map to track hashes that need removal
	targets := make(map[uint32]bool)
	for i := 0; i < h.replicas; i++ {
		virtualNode := node + "-" + strconv.Itoa(i)
		hash := h.hash(virtualNode)
		delete(h.hashMap, hash)
		targets[hash] = true
	}

	// Filter out removed hashes safely in a single pass
	newKeys := make([]uint32, 0, len(h.keys))
	for _, k := range h.keys {
		if !targets[k] {
			newKeys = append(newKeys, k)
		}
	}
	h.keys = newKeys
}

// get node
func (h *HashRing) GetNode(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.keys) == 0 {
		return ""
	}

	hash := h.hash(key)

	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})
	// wrap around
	if idx == len(h.keys) {
		idx = 0
	}
	return h.hashMap[h.keys[idx]]
}

// for ring visualization
func (h *HashRing) PrintRing() {

	h.mu.RLock()
	defer h.mu.RUnlock()

	fmt.Println("==== HASH RING ====")

	for _, key := range h.keys {
		fmt.Printf("%d -> %s\n", key, h.hashMap[key])
	}
}


