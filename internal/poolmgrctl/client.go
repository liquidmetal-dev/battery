package poolmgrctl

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// connFlags holds the connection-related flag values used to dial the pool
// manager's gRPC API.
type connFlags struct {
	addr     string
	insecure bool
	caFile   string
	certFile string
	keyFile  string
}

// dial builds transport credentials from cf and dials cf.addr, returning the
// resulting connection.
func dial(cf connFlags) (*grpc.ClientConn, error) {
	creds, err := dialCredentials(cf)
	if err != nil {
		return nil, err
	}
	return grpc.NewClient(cf.addr, grpc.WithTransportCredentials(creds))
}

// dialCredentials builds the gRPC transport credentials for cf: an explicit
// insecure mode, or TLS verifying the server via caFile and optionally
// presenting a client certificate (mTLS) via certFile/keyFile.
func dialCredentials(cf connFlags) (credentials.TransportCredentials, error) {
	if cf.insecure {
		return insecure.NewCredentials(), nil
	}

	if cf.caFile == "" {
		return nil, errors.New("either --ca-file or --insecure must be set")
	}

	caPEM, err := os.ReadFile(cf.caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca file: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("ca file %s: no certificates found", cf.caFile)
	}

	tlsConfig := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}

	if cf.certFile != "" {
		cert, err := tls.LoadX509KeyPair(cf.certFile, cf.keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return credentials.NewTLS(tlsConfig), nil
}
