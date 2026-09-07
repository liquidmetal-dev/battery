// Package grpcconfig holds shared server configuration, including the TLS setup pattern used by
// battery's gRPC servers (mirroring flintlock's own internal/config + pkg/auth conventions).
package grpcconfig

// Config is the shared configuration for a battery gRPC server.
type Config struct {
	ListenAddress string
	TLS           TLSConfig
}

// TLSConfig configures how a gRPC server authenticates its transport: either explicit insecure
// mode, or TLS with an optional mTLS client-certificate requirement.
type TLSConfig struct {
	Insecure       bool
	CertFile       string
	KeyFile        string
	ValidateClient bool
	ClientCAFile   string
}
