package transport

import (
	"cache/internals/transport/proto"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)

// Transport only needs these methods
type Cache interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	Delete(key string)
}

type CacheServer struct {
	proto.UnimplementedCacheServiceServer

	cache Cache
}

// NewCacheServer creates a new cache gRPC server
func NewCacheServer(c Cache) *CacheServer {
	return &CacheServer{
		cache: c,
	}
}

func (s *CacheServer) Get(
	ctx context.Context,
	req *proto.GetRequest,
) (*proto.GetResponse, error) {

	log.Printf(
		"[GRPC SERVER] GET key=%s\n",
		req.Key,
	)

	val, found := s.cache.Get(req.Key)

	if !found {

		log.Printf(
			"[GRPC SERVER] MISS key=%s\n",
			req.Key,
		)

		return &proto.GetResponse{
			Found: false,
		}, nil
	}

	data, err := json.Marshal(val)
	if err != nil {

		return nil, fmt.Errorf(
			"failed to serialize key=%s: %w",
			req.Key,
			err,
		)
	}

	log.Printf(
		"[GRPC SERVER] HIT key=%s\n",
		req.Key,
	)

	return &proto.GetResponse{
		Value: data,
		Found: true,
	}, nil
}

func (s *CacheServer) Set(
	ctx context.Context,
	req *proto.SetRequest,
) (*proto.SetResponse, error) {

	log.Printf(
		"[GRPC SERVER] SET key=%s\n",
		req.Key,
	)

	var value any

	err := json.Unmarshal(req.Value, &value)
	if err != nil {

		return nil, fmt.Errorf(
			"failed to deserialize key=%s: %w",
			req.Key,
			err,
		)
	}

	s.cache.Set(req.Key, value)

	log.Printf(
		"[GRPC SERVER] STORED key=%s\n",
		req.Key,
	)

	return &proto.SetResponse{
		Success: true,
	}, nil
}

func (s *CacheServer) Delete(
	ctx context.Context,
	req *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {

	log.Printf(
		"[GRPC SERVER] DELETE key=%s\n",
		req.Key,
	)

	s.cache.Delete(req.Key)

	log.Printf(
		"[GRPC SERVER] DELETED key=%s\n",
		req.Key,
	)

	return &proto.DeleteResponse{
		Success: true,
	}, nil
}

func StartGRPCServer(
	port string,
	cache Cache,
) error {

	lis, err := net.Listen("tcp", port)
	if err != nil {

		return fmt.Errorf(
			"failed to listen on port %s: %w",
			port,
			err,
		)
	}

	grpcServer := grpc.NewServer()

	proto.RegisterCacheServiceServer(
		grpcServer,
		NewCacheServer(cache),
	)

	log.Printf(
		"[GRPC SERVER] running on %s\n",
		port,
	)

	err = grpcServer.Serve(lis)

	if err != nil {

		return fmt.Errorf(
			"grpc server failed: %w",
			err,
		)
	}

	return nil
}
