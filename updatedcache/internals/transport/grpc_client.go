package transport

import (
	"cache/internals/transport/proto"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerClient struct {
	conn   *grpc.ClientConn
	client proto.CacheServiceClient
}

// NewPeerClient creates a new gRPC peer connection
func NewPeerClient(addr string) (*PeerClient, error) {

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		addr,

		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),

		grpc.WithBlock(),
	)

	if err != nil {
		return nil, fmt.Errorf(
			"failed to connect to peer %s: %w",
			addr,
			err,
		)
	}

	client := proto.NewCacheServiceClient(conn)

	log.Printf("[PEER] connected to %s\n", addr)

	return &PeerClient{
		conn:   conn,
		client: client,
	}, nil
}

// Close closes gRPC connection
func (p *PeerClient) Close() error {

	log.Println("[PEER] closing connection")

	return p.conn.Close()
}

// Get fetches value from remote node
func (p *PeerClient) Get(
	ctx context.Context,
	key string,
) (any, bool, error) {

	log.Printf(
		"[PEER] GET key=%s\n",
		key,
	)

	resp, err := p.client.Get(
		ctx,
		&proto.GetRequest{
			Key: key,
		},
	)

	if err != nil {
		return nil, false, fmt.Errorf(
			"grpc GET failed for key=%s: %w",
			key,
			err,
		)
	}

	if !resp.Found {

		log.Printf(
			"[PEER] MISS key=%s\n",
			key,
		)

		return nil, false, nil
	}

	var value any

	err = json.Unmarshal(resp.Value, &value)
	if err != nil {
		return nil, false, fmt.Errorf(
			"failed to deserialize key=%s: %w",
			key,
			err,
		)
	}

	log.Printf(
		"[PEER] HIT key=%s\n",
		key,
	)

	return value, true, nil
}

// Set stores value on remote node
func (p *PeerClient) Set(
	ctx context.Context,
	key string,
	value any,
) error {

	log.Printf(
		"[PEER] SET key=%s\n",
		key,
	)

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf(
			"failed to serialize key=%s: %w",
			key,
			err,
		)
	}

	_, err = p.client.Set(
		ctx,
		&proto.SetRequest{
			Key:   key,
			Value: data,
		},
	)

	if err != nil {
		return fmt.Errorf(
			"grpc SET failed for key=%s: %w",
			key,
			err,
		)
	}

	log.Printf(
		"[PEER] STORED key=%s\n",
		key,
	)

	return nil
}

// Delete removes value from remote node
func (p *PeerClient) Delete(
	ctx context.Context,
	key string,
) error {

	log.Printf(
		"[PEER] DELETE key=%s\n",
		key,
	)

	_, err := p.client.Delete(
		ctx,
		&proto.DeleteRequest{
			Key: key,
		},
	)

	if err != nil {
		return fmt.Errorf(
			"grpc DELETE failed for key=%s: %w",
			key,
			err,
		)
	}

	log.Printf(
		"[PEER] DELETED key=%s\n",
		key,
	)

	return nil
}
