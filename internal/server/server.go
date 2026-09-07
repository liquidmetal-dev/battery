// Package server constructs the pool manager's gRPC API server: TLS
// credentials (or an explicit insecure mode) plus an optional basic-auth
// interceptor, mirroring flintlock's own server auth pattern. Callers
// register their gRPC services on the returned *grpc.Server themselves.
package server

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/liquidmetal-dev/battery/internal/config"
)

// New builds a *grpc.Server configured per cfg: TLS credentials (or an
// explicit insecure mode) and, if cfg.BasicAuthToken is set, a basic-auth
// interceptor rejecting unauthenticated calls. extraOpts, if given, are
// appended after those (e.g. a caller's metrics.Registry.ServerOptions()).
// Callers still need to register their services on the returned server and
// Serve it.
func New(cfg config.APIServerConfig, extraOpts ...grpc.ServerOption) (*grpc.Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}

	var opts []grpc.ServerOption

	if !cfg.TLS.Insecure {
		creds, err := loadServerTLS(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("server: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	if cfg.BasicAuthToken != "" {
		unary, stream := basicAuthInterceptors(cfg.BasicAuthToken)
		opts = append(opts, grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream))
	}

	opts = append(opts, extraOpts...)

	return grpc.NewServer(opts...), nil
}

// loadServerTLS builds gRPC transport credentials from cfg: the server's
// own certificate/key, and, when cfg.ValidateClient is set, a client CA
// pool that gates the connection on a valid client certificate (mTLS).
func loadServerTLS(cfg config.ServerTLSConfig) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert/key: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if cfg.ValidateClient {
		caPEM, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read client ca file: %w", err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("client ca file %s: no certificates found", cfg.ClientCAFile)
		}

		tlsConfig.ClientCAs = pool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return credentials.NewTLS(tlsConfig), nil
}

var (
	errMissingAuth = status.Error(codes.Unauthenticated, "missing or malformed authorization header")
	errInvalidAuth = status.Error(codes.Unauthenticated, "invalid auth token")
)

// basicAuthInterceptors returns unary and stream server interceptors that
// require every call to carry a "basic <base64(token)>" authorization
// header matching token, comparing in constant time.
func basicAuthInterceptors(token string) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	expected := base64.StdEncoding.EncodeToString([]byte(token))

	check := func(ctx context.Context) error {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return errMissingAuth
		}
		values := md.Get("authorization")
		if len(values) == 0 {
			return errMissingAuth
		}
		const prefix = "basic "
		v := values[0]
		if len(v) < len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
			return errMissingAuth
		}
		supplied := v[len(prefix):]
		if subtle.ConstantTimeCompare([]byte(supplied), []byte(expected)) != 1 {
			return errInvalidAuth
		}
		return nil
	}

	unary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := check(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}

	stream := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := check(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}

	return unary, stream
}
