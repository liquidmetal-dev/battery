// Package flintlockclient manages one flintlock gRPC client per flintlock
// host.
package flintlockclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"

	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	microvmsshproxyv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmsshproxy/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// ErrUnknownHost is returned when asked for a host name that isn't in the
// pool.
var ErrUnknownHost = errors.New("flintlockclient: unknown host")

// ErrHostExists is returned by Pool.Add when the pool already holds a host
// with the same name.
var ErrHostExists = errors.New("flintlockclient: host already exists")

// ErrInvalidHost is returned by Dial and New when a host spec is malformed:
// a missing name or address, or an inconsistent TLS config.
var ErrInvalidHost = errors.New("flintlockclient: invalid host")

// Conn is one flintlock host's MicroVMClient, MicroVMExecClient, and
// MicroVMSSHProxyClient, all backed by the same grpc.ClientConn, plus the
// last version CheckVersion saw on it. Dial returns one that belongs to no
// Pool, so a caller can check a candidate host before handing it to
// Pool.Add or Pool.Update.
type Conn struct {
	name     string
	address  string
	conn     *grpc.ClientConn
	microVM  microvmv1alpha1.MicroVMClient
	exec     microvmexecv1alpha1.MicroVMExecClient
	sshProxy microvmsshproxyv1alpha1.MicroVMSSHProxyClient

	// version is the flintlock version the last ServerInfo reply reported,
	// and versionOK records that it met MinFlintlockVersion.
	versionMu sync.Mutex
	version   string
	versionOK bool
}

// Dial validates host and builds a Conn for it. grpc.NewClient doesn't
// block or connect, so an unreachable host only shows up on the first RPC
// (see Conn.CheckVersion); a bad spec or unreadable TLS file fails here.
func Dial(host *poolmgrv1alpha1.Host) (*Conn, error) {
	if err := validateHost(host); err != nil {
		return nil, err
	}

	creds, err := dialCredentials(host.GetTls())
	if err != nil {
		return nil, fmt.Errorf("flintlockclient: host %q: %w", host.GetName(), err)
	}

	conn, err := grpc.NewClient(host.GetAddress(), grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("flintlockclient: host %q: dial %s: %w", host.GetName(), host.GetAddress(), err)
	}

	return &Conn{
		name:     host.GetName(),
		address:  host.GetAddress(),
		conn:     conn,
		microVM:  microvmv1alpha1.NewMicroVMClient(conn),
		exec:     microvmexecv1alpha1.NewMicroVMExecClient(conn),
		sshProxy: microvmsshproxyv1alpha1.NewMicroVMSSHProxyClient(conn),
	}, nil
}

// Name returns the host name c was dialled for.
func (c *Conn) Name() string { return c.name }

// Close closes c's underlying connection.
func (c *Conn) Close() error { return c.conn.Close() }

// Pool holds one Conn per flintlock host, keyed by host name. Add, Update,
// and Remove may run concurrently with the readers. An RPC still in flight
// on a connection that Update or Remove closes fails with the connection.
type Pool struct {
	mu    sync.RWMutex
	hosts map[string]*Conn
}

// New dials every host in hosts and returns a Pool. An empty list is valid:
// hosts can be added later with Add. On any validation or dial error it
// closes what it already opened and returns the error.
func New(hosts []*poolmgrv1alpha1.Host) (*Pool, error) {
	p := &Pool{hosts: make(map[string]*Conn, len(hosts))}

	for _, host := range hosts {
		c, err := Dial(host)
		if err == nil {
			err = p.Add(c)
			if err != nil {
				_ = c.Close()
			}
		}
		if err != nil {
			_ = p.Close()
			return nil, err
		}
	}

	return p, nil
}

// Add puts c in the pool under c.Name(), returning an ErrHostExists-wrapping
// error if that name is taken. On success the pool owns c; on error the
// caller still does, and should Close it.
func (p *Pool) Add(c *Conn) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.hosts[c.name]; ok {
		return fmt.Errorf("%w: %q", ErrHostExists, c.name)
	}
	p.hosts[c.name] = c
	return nil
}

// Update replaces the connection for the host named c.Name() with c, then
// closes the old one. The version check starts over with c's own state, so
// unless the caller already ran c.CheckVersion the next provision on the
// host checks again. It returns an ErrUnknownHost-wrapping error if no such
// host is in the pool; the caller then still owns c.
func (p *Pool) Update(c *Conn) error {
	p.mu.Lock()
	old, ok := p.hosts[c.name]
	if ok {
		p.hosts[c.name] = c
	}
	p.mu.Unlock()

	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownHost, c.name)
	}
	if err := old.Close(); err != nil {
		return fmt.Errorf("flintlockclient: host %q: close old connection: %w", c.name, err)
	}
	return nil
}

// Remove drops the named host from the pool and closes its connection,
// returning an ErrUnknownHost-wrapping error if no such host is in the
// pool.
func (p *Pool) Remove(hostName string) error {
	p.mu.Lock()
	old, ok := p.hosts[hostName]
	delete(p.hosts, hostName)
	p.mu.Unlock()

	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownHost, hostName)
	}
	if err := old.Close(); err != nil {
		return fmt.Errorf("flintlockclient: host %q: close connection: %w", hostName, err)
	}
	return nil
}

// conn returns the Conn for the named host, or ErrUnknownHost.
func (p *Pool) conn(hostName string) (*Conn, error) {
	p.mu.RLock()
	c, ok := p.hosts[hostName]
	p.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownHost, hostName)
	}
	return c, nil
}

// Client returns the MicroVMClient for the named host, or ErrUnknownHost if
// no such host is in the pool.
func (p *Pool) Client(hostName string) (microvmv1alpha1.MicroVMClient, error) {
	c, err := p.conn(hostName)
	if err != nil {
		return nil, err
	}
	return c.microVM, nil
}

// ExecClient returns the MicroVMExecClient for the named host, or
// ErrUnknownHost if no such host is in the pool.
func (p *Pool) ExecClient(hostName string) (microvmexecv1alpha1.MicroVMExecClient, error) {
	c, err := p.conn(hostName)
	if err != nil {
		return nil, err
	}
	return c.exec, nil
}

// SSHProxyClient returns the MicroVMSSHProxyClient for the named host, or
// ErrUnknownHost if no such host is in the pool.
func (p *Pool) SSHProxyClient(hostName string) (microvmsshproxyv1alpha1.MicroVMSSHProxyClient, error) {
	c, err := p.conn(hostName)
	if err != nil {
		return nil, err
	}
	return c.sshProxy, nil
}

// Address returns the gRPC address for the named host, or ErrUnknownHost if
// no such host is in the pool.
func (p *Pool) Address(hostName string) (string, error) {
	c, err := p.conn(hostName)
	if err != nil {
		return "", err
	}
	return c.address, nil
}

// Hosts returns the names of the hosts in the pool.
func (p *Pool) Hosts() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	names := make([]string, 0, len(p.hosts))
	for name := range p.hosts {
		names = append(names, name)
	}
	return names
}

// Close closes all underlying connections, returning a joined error for any
// that failed to close.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	for _, c := range p.hosts {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// validateHost checks that host has a name and address and a consistent TLS
// config: insecure with no TLS files, or a ca_file plus an optional matched
// cert_file/key_file pair. It is the only place these rules live.
func validateHost(host *poolmgrv1alpha1.Host) error {
	if host == nil {
		return fmt.Errorf("%w: host is required", ErrInvalidHost)
	}
	if host.GetName() == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidHost)
	}
	if host.GetAddress() == "" {
		return fmt.Errorf("%w: host %q: address is required", ErrInvalidHost, host.GetName())
	}

	t := host.GetTls()
	if t.GetInsecure() {
		if t.GetCertFile() != "" || t.GetKeyFile() != "" || t.GetCaFile() != "" {
			return fmt.Errorf("%w: host %q: tls: insecure hosts must not set cert_file/key_file/ca_file", ErrInvalidHost, host.GetName())
		}
		return nil
	}
	if t.GetCaFile() == "" {
		return fmt.Errorf("%w: host %q: tls: ca_file is required unless insecure is set", ErrInvalidHost, host.GetName())
	}
	if (t.GetCertFile() == "") != (t.GetKeyFile() == "") {
		return fmt.Errorf("%w: host %q: tls: cert_file and key_file must be set together", ErrInvalidHost, host.GetName())
	}
	return nil
}

// dialCredentials builds the gRPC transport credentials for a host's TLS
// settings: an explicit insecure mode, or TLS verifying the server via
// CaFile and optionally presenting a client certificate (mTLS) via
// CertFile/KeyFile. The files are read on every call, so a rotated
// certificate takes effect on the next Dial.
func dialCredentials(t *poolmgrv1alpha1.HostTLS) (credentials.TransportCredentials, error) {
	if t.GetInsecure() {
		return insecure.NewCredentials(), nil
	}

	caPEM, err := os.ReadFile(t.GetCaFile())
	if err != nil {
		return nil, fmt.Errorf("read ca file: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("ca file %s: no certificates found", t.GetCaFile())
	}

	tlsConfig := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}

	if t.GetCertFile() != "" {
		cert, err := tls.LoadX509KeyPair(t.GetCertFile(), t.GetKeyFile())
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return credentials.NewTLS(tlsConfig), nil
}
