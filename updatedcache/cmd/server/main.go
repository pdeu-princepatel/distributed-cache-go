package main

import (
	cachepkg "cache/internals/cache"
	"cache/internals/cluster"
	"cache/internals/transport"
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"time"
)

// Global cache and router
var (
	c      *cachepkg.ShardedCache
	router *cluster.Router
)

// -----------------------------
// Cache Initialization
// -----------------------------
func init() {

	c = cachepkg.NewShardedCache(
		16,   // shards
		1000, // hot capacity
		100,  // warm capacity
		256,  // promotion threshold
		nil,  // eviction callback
		5*time.Minute,
	)
}

// -----------------------------
// Loader Function
// -----------------------------
func loader(
	ctx context.Context,
	key string,
) (interface{}, error) {

	log.Printf(
		"[LOADER] generating value for key=%s\n",
		key,
	)

	return "generated-value-" + key, nil
}

// -----------------------------
// HTTP Handlers
// -----------------------------
func getHandler(
	w http.ResponseWriter,
	r *http.Request,
) {

	key := r.URL.Query().Get("key")

	if key == "" {

		http.Error(
			w,
			"missing key",
			http.StatusBadRequest,
		)

		return
	}

	val, err := router.GetWithLoad(
		r.Context(),
		key,
		loader,
	)

	if err != nil {

		log.Printf(
			"[HTTP] GET failed key=%s err=%v\n",
			key,
			err,
		)

		_ = json.NewEncoder(w).Encode(
			map[string]interface{}{
				"key":    key,
				"error":  err.Error(),
				"status": "failed",
			},
		)

		return
	}

	_ = json.NewEncoder(w).Encode(
		map[string]interface{}{
			"key":   key,
			"value": val,
		},
	)
}

func statsHandler(
	w http.ResponseWriter,
	r *http.Request,
) {

	stats := c.Stats()

	total := stats.Hits + stats.Misses

	hitRate := 0.0

	if total > 0 {
		hitRate = float64(stats.Hits) / float64(total)
	}

	_ = json.NewEncoder(w).Encode(
		map[string]interface{}{
			"hits":       stats.Hits,
			"misses":     stats.Misses,
			"evictions":  stats.Evictions,
			"loads":      stats.Loads,
			"loadErrors": stats.LoadErrors,
			"hit_rate":   hitRate,
		},
	)
}

// -----------------------------
// MAIN
// -----------------------------
func main() {

	// -----------------------------
	// Flags
	// -----------------------------
	httpPort := flag.String(
		"http",
		"8080",
		"http server port",
	)

	grpcPort := flag.String(
		"grpc",
		"5001",
		"grpc server port",
	)

	flag.Parse()

	// -----------------------------
	// CPU Profiling
	// -----------------------------
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	pprof.StartCPUProfile(f)

	defer pprof.StopCPUProfile()

	// -----------------------------
	// Start Janitor
	// -----------------------------
	c.StartJanitor(30 * time.Second)

	defer c.StopJanitor()

	// -----------------------------
	// Cluster Setup
	// -----------------------------
	self := "localhost:" + *grpcPort

	ring := cluster.NewHashRing(10)

	ring.AddNode("localhost:5001")
	ring.AddNode("localhost:5002")
	ring.AddNode("localhost:5003")

	router = cluster.NewRouter(
		self,
		ring,
		c,
	)

	// -----------------------------
	// Start gRPC Server
	// -----------------------------
	go func() {

		log.Printf(
			"[GRPC] starting gRPC server on :%s\n",
			*grpcPort,
		)

		err := transport.StartGRPCServer(
			":"+*grpcPort,
			c,
		)

		if err != nil {
			log.Fatal(err)
		}

	}()

	// Wait a little for server startup
	time.Sleep(1 * time.Second)

	// -----------------------------
	// Peer Connections
	// -----------------------------

	peerAddresses := []string{
		"localhost:5001",
		"localhost:5002",
		"localhost:5003",
	}

	for _, addr := range peerAddresses {

		client, err := transport.NewPeerClient(addr)

		if err != nil {

			log.Printf(
				"[PEER] failed to connect to %s err=%v\n",
				addr,
				err,
			)

			continue
		}

		router.AddPeer(
			addr,
			client,
		)

		log.Printf(
			"[PEER] registered peer %s\n",
			addr,
		)
	}

	// -----------------------------
	// HTTP Routes
	// -----------------------------
	http.HandleFunc(
		"/",
		func(w http.ResponseWriter, r *http.Request) {

			w.Write(
				[]byte(
					"Distributed Cache Server Running",
				),
			)
		},
	)

	http.HandleFunc("/get", getHandler)

	http.HandleFunc("/stats", statsHandler)

	// -----------------------------
	// Startup Logs
	// -----------------------------
	log.Printf(
		"[HTTP] server running at http://localhost:%s\n",
		*httpPort,
	)

	log.Printf(
		"[NODE] self=%s\n",
		self,
	)

	log.Printf(
		"[PPROF] available at http://localhost:%s/debug/pprof/\n",
		*httpPort,
	)

	// -----------------------------
	// Start HTTP Server
	// -----------------------------
	log.Fatal(
		http.ListenAndServe(
			":"+*httpPort,
			nil,
		),
	)
}
