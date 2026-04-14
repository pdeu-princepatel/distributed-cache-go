package main

import (
	cachepkg "cache/internals/cache"
	"context"
	"encoding/json"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"time"
)

var c = cachepkg.NewShardedCache(
	16,   // shards
	1000, // hot capacity
	100,  // warm capacity
	256,  // promotion threshold
	nil,  // eviction callback
	5*time.Minute,
)

func loader(ctx context.Context, key string) (interface{}, error) {
	return "generated-value-" + key, nil
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	val, err := c.GetWithLoad(context.Background(), key, loader)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"key":   key,
		"value": val,
	})
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	stats := c.Stats()

	total := stats.Hits + stats.Misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(stats.Hits) / float64(total)
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"hits":       stats.Hits,
		"misses":     stats.Misses,
		"evictions":  stats.Evictions,
		"loads":      stats.Loads,
		"loadErrors": stats.LoadErrors,
		"hit_rate":   hitRate,
	})
}

func main() {

	// -------- CPU Profiling --------
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// -------- Start Background Cleaner --------
	c.StartJanitor(30 * time.Second)
	defer c.StopJanitor()

	// -------- Routes --------
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Cache Server Running... "))
	})
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/stats", statsHandler)

	log.Println("Server running at http://localhost:8080")
	log.Println("pprof available at http://localhost:8080/debug/pprof/")

	// -------- Start Server --------
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// http://localhost:8080/debug/pprof/
// for ($i=0; $i -lt 5000; $i++) {
//   curl "http://localhost:8080/get?key=test"
// }
