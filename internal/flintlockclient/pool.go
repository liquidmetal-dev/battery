// Package flintlockclient manages one flintlock gRPC client per configured
// flintlock host.
package flintlockclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/liquidmetal-dev/battery/internal/config"
)

// ErrUnknownHost is returned by Pool.Client when asked for a host name that
// isn't in the pool's configuration.
var ErrUnknownHost = errors.New("flintlockclient: unknown host")

// Pool holds one MicroVMClient (backed by one grpc.ClientConn) per
// configured flintlock host, keyed by the host's config Name.
type Pool struct {
	conns   map[string]*grpc.ClientConn
	clients map[string]microvmv1alpha1.MicroVMClient
}

// New dials every host in cfg and returns a Pool. On any dial/TLS-setup
// error it closes what it already opened and returns the error.
func New(cfg *config.Config) (*Pool, error) {
	p := &Pool{
		conns:   make(map[string]*grpc.ClientConn, len(cfg.Hosts)),
		clients: make(map[string]microvmv1alpha1.MicroVMClient, len(cfg.Hosts)),
	}

	for _, host := range cfg.Hosts {
		creds, err := dialCredentials(host.TLS)
		if err != nil {
			_ = p.Close()
			return nil, fmt.Errorf("flintlockclient: host %q: %w", host.Name, err)
		}

		conn, err := grpc.NewClient(host.Address, grpc.WithTransportCredentials(creds))
		if err != nil {
			_ = p.Close()
			return nil, fmt.Errorf("flintlockclient: host %q: dial %s: %w", host.Name, host.Address, err)
		}

		p.conns[host.Name] = conn
		p.clients[host.Name] = microvmv1alpha1.NewMicroVMClient(conn)
	}

	return p, nil
}

// Client returns the MicroVMClient for the named host, or ErrUnknownHost if
// no such host was configured.
func (p *Pool) Client(hostName string) (microvmv1alpha1.MicroVMClient, error) {
	client, ok := p.clients[hostName]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownHost, hostName)
	}
	return client, nil
}

// Hosts returns the configured host names.
func (p *Pool) Hosts() []string {
	names := make([]string, 0, len(p.clients))
	for name := range p.clients {
		names = append(names, name)
	}
	return names
}

// Close closes all underlying connections, returning a joined error for any
// that failed to close.
func (p *Pool) Close() error {
	var errs []error
	for _, conn := range p.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// dialCredentials builds the gRPC transport credentials for a host's TLS
// config: an explicit insecure mode, or TLS verifying the server via CAFile
// and optionally presenting a client certificate (mTLS) via
// CertFile/KeyFile.
func dialCredentials(t config.TLSConfig) (credentials.TransportCredentials, error) {
	if t.Insecure {
		return insecure.NewCredentials(), nil
	}

	caPEM, err := os.ReadFile(t.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read ca file: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("ca file %s: no certificates found", t.CAFile)
	}

	tlsConfig := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}

	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return credentials.NewTLS(tlsConfig), nil
}
