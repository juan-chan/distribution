package grpc

import (
	"sync"
	"time"

	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
)

// ConnPool is a gRPC client connection pool.
type ConnPool struct {
	sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewConnPool creates a default gRPC client connection pool.
func NewConnPool() *ConnPool {
	return &ConnPool{
		conns: make(map[string]*grpc.ClientConn),
	}
}

// Get returns a gRPC Client Connection from the pool of the given target.
func (c *ConnPool) Get(target string) (*grpc.ClientConn, error) {
	c.Lock()
	defer c.Unlock()

	if conn, ok := c.conns[target]; ok {
		return conn, nil
	}

	grpcRetryOpts := []grpcretry.CallOption{
		grpcretry.WithCodes(grpcretry.DefaultRetriableCodes...),
		grpcretry.WithBackoff(grpcretry.BackoffExponential(100 * time.Millisecond)),
		grpcretry.WithMax(3),
		grpcretry.WithPerRetryTimeout(time.Second * 3),
	}

	// turns on recording of handling time of RPCs
	grpcprometheus.EnableClientHandlingTimeHistogram()

	conn, err := grpc.Dial(
		target,
		grpc.WithInsecure(),
		grpc.WithChainUnaryInterceptor(
			grpcretry.UnaryClientInterceptor(grpcRetryOpts...),
			grpcprometheus.UnaryClientInterceptor,
		),
		grpc.WithChainStreamInterceptor(
			grpcretry.StreamClientInterceptor(grpcRetryOpts...),
			grpcprometheus.StreamClientInterceptor,
		),
	)

	if err != nil {
		return nil, err
	}
	c.conns[target] = conn

	return conn, nil
}

// Close closes all the connection in the pool.
func (c *ConnPool) Close() {
	for _, conn := range c.conns {
		if conn != nil {
			conn.Close()
		}
	}
}
