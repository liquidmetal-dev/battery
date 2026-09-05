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
	"testing"
	"time"

	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	"github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/liquidmetal-dev/battery/internal/config"
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
// listener (loopback), optionally with TLS creds, and returns its address
// and a cleanup func.
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

func TestPool_InsecureRoundTrip(t *testing.T) {
	addr := startFakeServer(t, nil, 7)

	cfg := &config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: addr, TLS: config.TLSConfig{Insecure: true}},
	}}

	pool, err := flintlockclient.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	client, err := pool.Client("host-a")
	if err != nil {
		t.Fatalf("Client: %v", err)
	}

	resp, err := client.GetMicroVM(context.Background(), &microvmv1alpha1.GetMicroVMRequest{Uid: "vm-1"})
	if err != nil {
		t.Fatalf("GetMicroVM: %v", err)
	}
	if resp.GetMicrovm().GetVersion() != 7 {
		t.Fatalf("expected version 7, got %d", resp.GetMicrovm().GetVersion())
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

	cfg := &config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: addr, TLS: config.TLSConfig{CAFile: certPath}},
	}}

	pool, err := flintlockclient.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	client, err := pool.Client("host-a")
	if err != nil {
		t.Fatalf("Client: %v", err)
	}

	resp, err := client.GetMicroVM(context.Background(), &microvmv1alpha1.GetMicroVMRequest{Uid: "vm-1"})
	if err != nil {
		t.Fatalf("GetMicroVM: %v", err)
	}
	if resp.GetMicrovm().GetVersion() != 9 {
		t.Fatalf("expected version 9, got %d", resp.GetMicrovm().GetVersion())
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

	cfg := &config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: addr, TLS: config.TLSConfig{CAFile: wrongCertPath}},
	}}

	pool, err := flintlockclient.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

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
	addr := startFakeServer(t, nil, 1)
	cfg := &config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: addr, TLS: config.TLSConfig{Insecure: true}},
	}}

	pool, err := flintlockclient.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	if _, err := pool.Client("does-not-exist"); !errors.Is(err, flintlockclient.ErrUnknownHost) {
		t.Fatalf("expected ErrUnknownHost, got %v", err)
	}
}

func TestPool_Hosts(t *testing.T) {
	addrA := startFakeServer(t, nil, 1)
	addrB := startFakeServer(t, nil, 1)

	cfg := &config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: addrA, TLS: config.TLSConfig{Insecure: true}},
		{Name: "host-b", Address: addrB, TLS: config.TLSConfig{Insecure: true}},
	}}

	pool, err := flintlockclient.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	got := map[string]bool{}
	for _, h := range pool.Hosts() {
		got[h] = true
	}
	if !got["host-a"] || !got["host-b"] || len(got) != 2 {
		t.Fatalf("unexpected hosts: %v", pool.Hosts())
	}
}

func TestPool_Close(t *testing.T) {
	addr := startFakeServer(t, nil, 1)
	cfg := &config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: addr, TLS: config.TLSConfig{Insecure: true}},
	}}

	pool, err := flintlockclient.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPool_New_InvalidTLSConfig(t *testing.T) {
	cfg := &config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: "127.0.0.1:0", TLS: config.TLSConfig{CAFile: filepath.Join(t.TempDir(), "missing-ca.pem")}},
	}}

	if _, err := flintlockclient.New(cfg); err == nil {
		t.Fatalf("expected error for unreadable CA file")
	}
}
