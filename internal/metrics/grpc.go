package metrics

import (
	"google.golang.org/grpc"
)

// ServerOptions returns the grpc.ServerOptions a caller should pass to
// grpc.NewServer to record standard gRPC server metrics (request counts,
// latency, in-flight streams) via grpc_prometheus, matching flintlock's own
// convention. RegisterGRPCServer must also be called, once the grpc.Server
// exists, so its methods are pre-registered (giving zero-valued series
// before the first call, rather than only appearing after it).
func (r *Registry) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StreamInterceptor(r.grpcMetrics.StreamServerInterceptor()),
		grpc.UnaryInterceptor(r.grpcMetrics.UnaryServerInterceptor()),
	}
}

// RegisterGRPCServer pre-registers srv's gRPC methods with the registry's
// gRPC metrics. Call once, after registering srv's services and before
// Serve, using a *grpc.Server constructed with this same Registry's
// ServerOptions.
func (r *Registry) RegisterGRPCServer(srv *grpc.Server) {
	r.grpcMetrics.InitializeMetrics(srv)
}
