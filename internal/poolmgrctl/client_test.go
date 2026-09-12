package poolmgrctl

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// genSelfSignedCert generates a self-signed CA-like cert/key pair for
// 127.0.0.1, writes them as PEM files under a temp dir, and returns their
// paths. Mirrors internal/flintlockclient/pool_test.go's fixture.
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
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
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

// startFakeServer starts a gRPC health server on a real TCP listener
// (loopback), optionally with TLS creds, and returns its address.
func startFakeServer(t *testing.T, creds credentials.TransportCredentials) string {
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
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(srv, healthSrv)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

func checkHealth(ctx context.Context, conn *grpc.ClientConn) error {
	client := grpc_health_v1.NewHealthClient(conn)
	_, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	return err
}

func TestDialCredentials_Insecure(t *testing.T) {
	addr := startFakeServer(t, nil)

	conn, err := dial(connFlags{addr: addr, insecure: true})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := checkHealth(ctx, conn); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
}

func TestDialCredentials_CAOnly(t *testing.T) {
	certPath, keyPath := genSelfSignedCert(t)

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load server cert: %v", err)
	}
	serverCreds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})

	addr := startFakeServer(t, serverCreds)

	conn, err := dial(connFlags{addr: addr, caFile: certPath})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := checkHealth(ctx, conn); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
}

func TestDialCredentials_WrongCA(t *testing.T) {
	certPath, keyPath := genSelfSignedCert(t)
	wrongCertPath, _ := genSelfSignedCert(t)

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load server cert: %v", err)
	}
	serverCreds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})

	addr := startFakeServer(t, serverCreds)

	conn, err := dial(connFlags{addr: addr, caFile: wrongCertPath})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := checkHealth(ctx, conn); err == nil {
		t.Fatal("expected error dialing with wrong CA, got nil")
	}
}

func TestDialCredentials_ClientCert(t *testing.T) {
	certPath, keyPath := genSelfSignedCert(t)

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load server cert: %v", err)
	}
	clientCAPool := x509.NewCertPool()
	clientPEM, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("read client cert: %v", err)
	}
	clientCAPool.AppendCertsFromPEM(clientPEM)

	serverCreds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    clientCAPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	})

	addr := startFakeServer(t, serverCreds)

	conn, err := dial(connFlags{addr: addr, caFile: certPath, certFile: certPath, keyFile: keyPath})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := checkHealth(ctx, conn); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
}

func TestDialCredentials_MissingCAFile(t *testing.T) {
	if _, err := dialCredentials(connFlags{caFile: "/nonexistent/ca.pem"}); err == nil {
		t.Fatal("expected error for missing ca file, got nil")
	}
}

// TestDialCredentials_NoTLSFlags_ClearError proves that dialing with
// neither --insecure nor --ca-file set (the first-run/no-flags case)
// produces a clear, actionable error instead of the confusing
// "read ca file: open : no such file or directory" that os.ReadFile("")
// would otherwise surface.
func TestDialCredentials_NoTLSFlags_ClearError(t *testing.T) {
	_, err := dialCredentials(connFlags{})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "either --ca-file or --insecure must be set") {
		t.Errorf("error = %q, want it to mention either --ca-file or --insecure must be set", err.Error())
	}
}
