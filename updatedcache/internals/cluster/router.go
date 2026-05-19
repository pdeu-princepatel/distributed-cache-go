package cluster

import (
	"context"
	"fmt"
	"log"

	"cache/internals/transport"
)

type Router struct {
	self string
	ring *HashRing

	cache Cache

	peers map[string]*transport.PeerClient
}

type Cache interface {
	Get(key string) (any, bool)

	Set(key string, value any)

	GetWithLoad(
		ctx context.Context,
		key string,
		loader func(context.Context, string) (any, error),
	) (any, error)
}

// NewRouter creates a new router instance
func NewRouter(
	self string,
	ring *HashRing,
	cache Cache,
) *Router {

	return &Router{
		self:  self,
		ring:  ring,
		cache: cache,
		peers: make(map[string]*transport.PeerClient),
	}
}

// ------------------------------------
// GET
// ------------------------------------
func (r *Router) Get(
	ctx context.Context,
	key string,
) (any, bool) {

	owner := r.ring.GetNode(key)

	log.Printf(
		"[ROUTER] GET key=%s owner=%s self=%s\n",
		key,
		owner,
		r.self,
	)

	// Local node
	if owner == r.self {

		log.Printf(
			"[ROUTER] local GET key=%s\n",
			key,
		)

		return r.cache.Get(key)
	}

	// Remote node
	peer, exists := r.peers[owner]

	if !exists {

		log.Printf(
			"[ROUTER] peer not found owner=%s\n",
			owner,
		)

		return nil, false
	}

	log.Printf(
		"[ROUTER] forwarding GET key=%s to=%s\n",
		key,
		owner,
	)

	val, found, err := peer.Get(
		ctx,
		key,
	)

	if err != nil {

		log.Printf(
			"[ROUTER] remote GET failed key=%s err=%v\n",
			key,
			err,
		)

		return nil, false
	}

	return val, found
}

// ------------------------------------
// SET
// ------------------------------------
func (r *Router) Set(
	ctx context.Context,
	key string,
	value any,
) {

	owner := r.ring.GetNode(key)

	log.Printf(
		"[ROUTER] SET key=%s owner=%s self=%s\n",
		key,
		owner,
		r.self,
	)

	// Local node
	if owner == r.self {

		log.Printf(
			"[ROUTER] local SET key=%s\n",
			key,
		)

		r.cache.Set(key, value)

		return
	}

	// Remote node
	peer, exists := r.peers[owner]

	if !exists {

		log.Printf(
			"[ROUTER] peer not found owner=%s\n",
			owner,
		)

		return
	}

	log.Printf(
		"[ROUTER] forwarding SET key=%s to=%s\n",
		key,
		owner,
	)

	err := peer.Set(
		ctx,
		key,
		value,
	)

	if err != nil {

		log.Printf(
			"[ROUTER] remote SET failed key=%s err=%v\n",
			key,
			err,
		)
	}
}

// ------------------------------------
// GET WITH LOAD
// ------------------------------------
func (r *Router) GetWithLoad(
	ctx context.Context,
	key string,
	loader func(context.Context, string) (any, error),
) (any, error) {

	owner := r.ring.GetNode(key)

	log.Printf(
		"[ROUTER] LOAD key=%s owner=%s self=%s\n",
		key,
		owner,
		r.self,
	)

	// Local ownership
	if owner == r.self {

		log.Printf(
			"[ROUTER] local LOAD key=%s\n",
			key,
		)

		return r.cache.GetWithLoad(
			ctx,
			key,
			loader,
		)
	}

	// Remote ownership
	peer, exists := r.peers[owner]

	if !exists {
		return nil, fmt.Errorf(
			"peer not found: %s",
			owner,
		)
	}

	log.Printf(
		"[ROUTER] forwarding LOAD key=%s to=%s\n",
		key,
		owner,
	)

	val, found, err := peer.Get(
		ctx,
		key,
	)

	if err != nil {
		return nil, err
	}

	if found {
		return val, nil
	}

	return nil, fmt.Errorf(
		"key not found remotely: %s",
		key,
	)
}

// ------------------------------------
// ADD PEER
// ------------------------------------
func (r *Router) AddPeer(
	node string,
	client *transport.PeerClient,
) {

	log.Printf(
		"[ROUTER] added peer=%s\n",
		node,
	)

	r.peers[node] = client
}
