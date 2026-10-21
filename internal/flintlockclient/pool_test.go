package flintlockclient_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	"github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
)

// fakeMicroVMServer is a minimal flintlock MicroVM service used to exercise
// the client pool's RPC path without a real flintlock/Firecracker host.
type fakeMicroVMServer struct {
	microvmv1alpha1.UnimplementedMicroVMServer
	version int32
}

func (f *fakeMicroVMServer) GetMicroVM(_ context.Context, _ *microvmv1alpha1.GetMicroVMRequest) (*microvmv1alpha1.GetMicroVMResponse, error) {
	return &microvmv1alpha1.GetMicroVMResponse{
		Microvm: &types.MicroVM{Version: f.version},
	}, nil
}

// startFakeServer starts a fake flintlock gRPC server on a real TCP
// listener (loopback), optionally with TLS creds, and returns its address.
// The server is stopped via t.Cleanup.
func startFakeServer(t *testing.T, creds credentials.TransportCredentials, version int32) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var opts []grpc.ServerOption
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}
	srv := grpc.NewServer(opts...)
	microvmv1alpha1.RegisterMicroVMServer(srv, &fakeMicroVMServer{version: version})

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// genSelfSignedCert generates a self-signed CA-like cert/key pair for
// 127.0.0.1, writes them as PEM files under a temp dir, and returns their
// paths.
func genSelfSignedCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	return certPath, keyPath
}

// insecureHost returns an insecure host spec named name at addr.
func insecureHost(name, addr string) *poolmgrv1alpha1.Host {
	return &poolmgrv1alpha1.Host{Name: name, Address: addr, Tls: &poolmgrv1alpha1.HostTLS{Insecure: true}}
}

// newPool calls New with hosts and closes the pool via t.Cleanup.
func newPool(t *testing.T, hosts ...*poolmgrv1alpha1.Host) *flintlockclient.Pool {
	t.Helper()

	pool, err := flintlockclient.New(hosts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

// dial calls Dial with host, failing the test on error.
func dial(t *testing.T, host *poolmgrv1alpha1.Host) *flintlockclient.Conn {
	t.Helper()

	c, err := flintlockclient.Dial(host)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return c
}

// microVMVersion calls GetMicroVM on the named host's client and returns
// the version the fake server reports.
func microVMVersion(t *testing.T, pool *flintlockclient.Pool, hostName string) int32 {
	t.Helper()

	client, err := pool.Client(hostName)
	if err != nil {
		t.Fatalf("Client(%q): %v", hostName, err)
	}
	resp, err := client.GetMicroVM(context.Background(), &microvmv1alpha1.GetMicroVMRequest{Uid: "vm-1"})
	if err != nil {
		t.Fatalf("GetMicroVM: %v", err)
	}
	return resp.GetMicrovm().GetVersion()
}

func TestPool_InsecureRoundTrip(t *testing.T) {
	addr := startFakeServer(t, nil, 7)
	pool := newPool(t, insecureHost("host-a", addr))

	if got := microVMVersion(t, pool, "host-a"); got != 7 {
		t.Fatalf("expected version 7, got %d", got)
	}
}

func TestPool_TLSRoundTrip(t *testing.T) {
	certPath, keyPath := genSelfSignedCert(t)

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load server cert: %v", err)
	}
	serverCreds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})

	addr := startFakeServer(t, serverCreds, 9)

	pool := newPool(t, &poolmgrv1alpha1.Host{
		Name: "host-a", Address: addr, Tls: &poolmgrv1alpha1.HostTLS{CaFile: certPath},
	})

	if got := microVMVersion(t, pool, "host-a"); got != 9 {
		t.Fatalf("expected version 9, got %d", got)
	}
}

func TestPool_TLSRoundTrip_WrongCA(t *testing.T) {
	certPath, keyPath := genSelfSignedCert(t)
	wrongCertPath, _ := genSelfSignedCert(t)

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load server cert: %v", err)
	}
	serverCreds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})

	addr := startFakeServer(t, serverCreds, 9)

	pool := newPool(t, &poolmgrv1alpha1.Host{
		Name: "host-a", Address: addr, Tls: &poolmgrv1alpha1.HostTLS{CaFile: wrongCertPath},
	})

	client, err := pool.Client("host-a")
	if err != nil {
		t.Fatalf("Client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := client.GetMicroVM(ctx, &microvmv1alpha1.GetMicroVMRequest{Uid: "vm-1"}); err == nil {
		t.Fatalf("expected error dialing with wrong CA, got nil")
	}
}

func TestPool_ClientUnknownHost(t *testing.T) {
	pool := newPool(t, insecureHost("host-a", startFakeServer(t, nil, 1)))

	if _, err := pool.Client("does-not-exist"); !errors.Is(err, flintlockclient.ErrUnknownHost) {
		t.Fatalf("expected ErrUnknownHost, got %v", err)
	}
}

func TestPool_Address(t *testing.T) {
	addr := startFakeServer(t, nil, 1)
	pool := newPool(t, insecureHost("host-a", addr))

	got, err := pool.Address("host-a")
	if err != nil {
		t.Fatalf("Address: %v", err)
	}
	if got != addr {
		t.Fatalf("expected address %s, got %s", addr, got)
	}

	if _, err := pool.Address("does-not-exist"); !errors.Is(err, flintlockclient.ErrUnknownHost) {
		t.Fatalf("expected ErrUnknownHost, got %v", err)
	}
}

func TestPool_Hosts(t *testing.T) {
	pool := newPool(t,
		insecureHost("host-a", startFakeServer(t, nil, 1)),
		insecureHost("host-b", startFakeServer(t, nil, 1)),
	)

	got := pool.Hosts()
	slices.Sort(got)
	if !slices.Equal(got, []string{"host-a", "host-b"}) {
		t.Fatalf("unexpected hosts: %v", got)
	}
}

func TestPool_Close(t *testing.T) {
	pool, err := flintlockclient.New([]*poolmgrv1alpha1.Host{insecureHost("host-a", startFakeServer(t, nil, 1))})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPool_New_NoHosts(t *testing.T) {
	pool := newPool(t)
	if got := pool.Hosts(); len(got) != 0 {
		t.Fatalf("Hosts() = %v, want none", got)
	}
}

func TestPool_New_DuplicateHost(t *testing.T) {
	_, err := flintlockclient.New([]*poolmgrv1alpha1.Host{
		insecureHost("host-a", "127.0.0.1:1"),
		insecureHost("host-a", "127.0.0.1:2"),
	})
	if !errors.Is(err, flintlockclient.ErrHostExists) {
		t.Fatalf("New() = %v, want ErrHostExists", err)
	}
}

func TestDial_InvalidHost(t *testing.T) {
	tests := []struct {
		name string
		host *poolmgrv1alpha1.Host
	}{
		{name: "nil host", host: nil},
		{name: "no name", host: insecureHost("", "127.0.0.1:1")},
		{name: "no address", host: insecureHost("host-a", "")},
		{name: "no tls", host: &poolmgrv1alpha1.Host{Name: "host-a", Address: "127.0.0.1:1"}},
		{name: "insecure with ca file", host: &poolmgrv1alpha1.Host{
			Name: "host-a", Address: "127.0.0.1:1", Tls: &poolmgrv1alpha1.HostTLS{Insecure: true, CaFile: "ca.pem"},
		}},
		{name: "cert without key", host: &poolmgrv1alpha1.Host{
			Name: "host-a", Address: "127.0.0.1:1", Tls: &poolmgrv1alpha1.HostTLS{CaFile: "ca.pem", CertFile: "cert.pem"},
		}},
		{name: "key without cert", host: &poolmgrv1alpha1.Host{
			Name: "host-a", Address: "127.0.0.1:1", Tls: &poolmgrv1alpha1.HostTLS{CaFile: "ca.pem", KeyFile: "key.pem"},
		}},
		{name: "insecure with cert file", host: &poolmgrv1alpha1.Host{
			Name: "host-a", Address: "127.0.0.1:1", Tls: &poolmgrv1alpha1.HostTLS{Insecure: true, CertFile: "cert.pem"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := flintlockclient.Dial(tt.host); !errors.Is(err, flintlockclient.ErrInvalidHost) {
				t.Fatalf("Dial() = %v, want ErrInvalidHost", err)
			}
			if _, err := flintlockclient.New([]*poolmgrv1alpha1.Host{tt.host}); !errors.Is(err, flintlockclient.ErrInvalidHost) {
				t.Fatalf("New() = %v, want ErrInvalidHost", err)
			}
		})
	}
}

func TestDial_UnreadableCAFile(t *testing.T) {
	_, err := flintlockclient.Dial(&poolmgrv1alpha1.Host{
		Name:    "host-a",
		Address: "127.0.0.1:0",
		Tls:     &poolmgrv1alpha1.HostTLS{CaFile: filepath.Join(t.TempDir(), "missing-ca.pem")},
	})
	if err == nil || errors.Is(err, flintlockclient.ErrInvalidHost) {
		t.Fatalf("Dial() = %v, want an unreadable CA file error", err)
	}
}

func TestPool_Add(t *testing.T) {
	pool := newPool(t, insecureHost("host-a", startFakeServer(t, nil, 1)))

	if err := pool.Add(dial(t, insecureHost("host-b", startFakeServer(t, nil, 2)))); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if got := microVMVersion(t, pool, "host-b"); got != 2 {
		t.Fatalf("host-b: expected version 2, got %d", got)
	}

	dup := dial(t, insecureHost("host-a", startFakeServer(t, nil, 3)))
	t.Cleanup(func() { _ = dup.Close() })
	if err := pool.Add(dup); !errors.Is(err, flintlockclient.ErrHostExists) {
		t.Fatalf("Add(duplicate) = %v, want ErrHostExists", err)
	}
	if got := microVMVersion(t, pool, "host-a"); got != 1 {
		t.Fatalf("host-a after duplicate Add: expected version 1, got %d", got)
	}
}

func TestPool_Update(t *testing.T) {
	oldAddr := startFakeServer(t, nil, 1)
	newAddr := startFakeServer(t, nil, 2)
	pool := newPool(t, insecureHost("host-a", oldAddr))

	oldClient, err := pool.Client("host-a")
	if err != nil {
		t.Fatalf("Client: %v", err)
	}

	if err := pool.Update(dial(t, insecureHost("host-a", newAddr))); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if got, _ := pool.Address("host-a"); got != newAddr {
		t.Fatalf("Address() after Update = %s, want %s", got, newAddr)
	}
	if got := microVMVersion(t, pool, "host-a"); got != 2 {
		t.Fatalf("expected version 2 from the new address, got %d", got)
	}

	// The old connection is closed, so a client taken before Update fails.
	if _, err := oldClient.GetMicroVM(context.Background(), &microvmv1alpha1.GetMicroVMRequest{Uid: "vm-1"}); err == nil {
		t.Fatalf("GetMicroVM on the old connection succeeded, want an error")
	}

	unknown := dial(t, insecureHost("host-b", newAddr))
	t.Cleanup(func() { _ = unknown.Close() })
	if err := pool.Update(unknown); !errors.Is(err, flintlockclient.ErrUnknownHost) {
		t.Fatalf("Update(unknown) = %v, want ErrUnknownHost", err)
	}
}

func TestPool_Remove(t *testing.T) {
	pool := newPool(t, insecureHost("host-a", startFakeServer(t, nil, 1)))

	oldClient, err := pool.Client("host-a")
	if err != nil {
		t.Fatalf("Client: %v", err)
	}

	if err := pool.Remove("host-a"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := pool.Client("host-a"); !errors.Is(err, flintlockclient.ErrUnknownHost) {
		t.Fatalf("Client() after Remove = %v, want ErrUnknownHost", err)
	}
	if got := pool.Hosts(); len(got) != 0 {
		t.Fatalf("Hosts() after Remove = %v, want none", got)
	}
	if _, err := oldClient.GetMicroVM(context.Background(), &microvmv1alpha1.GetMicroVMRequest{Uid: "vm-1"}); err == nil {
		t.Fatalf("GetMicroVM on the removed connection succeeded, want an error")
	}

	if err := pool.Remove("host-a"); !errors.Is(err, flintlockclient.ErrUnknownHost) {
		t.Fatalf("Remove(unknown) = %v, want ErrUnknownHost", err)
	}
}

// TestPool_ConcurrentReadsAndMutations is meant for -race: readers look up
// every kind of client while another goroutine adds, updates, and removes
// hosts.
func TestPool_ConcurrentReadsAndMutations(t *testing.T) {
	addr := startFakeServer(t, nil, 1)
	pool := newPool(t, insecureHost("host-a", addr))

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				for _, name := range []string{"host-a", "host-b"} {
					_, _ = pool.Client(name)
					_, _ = pool.ExecClient(name)
					_, _ = pool.SSHProxyClient(name)
					_, _ = pool.Address(name)
					_, _ = pool.FlintlockVersion(name)
				}
				_ = pool.CheckVersion(ctx, "host-a")
				_ = pool.Hosts()
			}
		}()
	}

	for i := range 100 {
		b := dial(t, insecureHost("host-b", addr))
		if err := pool.Add(b); err != nil {
			t.Errorf("Add #%d: %v", i, err)
			_ = b.Close()
		}
		if err := pool.Update(dial(t, insecureHost("host-a", addr))); err != nil {
			t.Errorf("Update #%d: %v", i, err)
		}
		if err := pool.Remove("host-b"); err != nil {
			t.Errorf("Remove #%d: %v", i, err)
		}
	}

	cancel()
	wg.Wait()
}
