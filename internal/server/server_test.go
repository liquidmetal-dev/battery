package server_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/server"
)

// fakeMicroVMServer is a minimal flintlock-shaped service used to exercise
// a constructed server's transport (TLS/auth) without needing any of
// battery's own service implementations.
type fakeMicroVMServer struct {
	microvmv1alpha1.UnimplementedMicroVMServer
}

func (f *fakeMicroVMServer) GetMicroVM(_ context.Context, _ *microvmv1alpha1.GetMicroVMRequest) (*microvmv1alpha1.GetMicroVMResponse, error) {
	return &microvmv1alpha1.GetMicroVMResponse{Microvm: &types.MicroVM{Version: 1}}, nil
}

// genSelfSignedCert generates a self-signed, CA-like ECDSA cert/key pair for
// 127.0.0.1, writes them as PEM files under a temp dir, and returns their
// paths. Mirrors internal/flintlockclient/pool_test.go's helper: the same
// cert doubles as its own "CA" for verifying the other side.
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

// startServer builds a *grpc.Server via server.New(cfg), registers
// fakeMicroVMServer on it, serves it on a loopback TCP listener, and
// returns its address. The server is stopped via t.Cleanup.
func startServer(t *testing.T, cfg config.APIServerConfig) string {
	t.Helper()

	srv, err := server.New(cfg)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	microvmv1alpha1.RegisterMicroVMServer(srv, &fakeMicroVMServer{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

func dial(t *testing.T, addr string, creds credentials.TransportCredentials) microvmv1alpha1.MicroVMClient {
	t.Helper()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return microvmv1alpha1.NewMicroVMClient(conn)
}

func callGetMicroVM(ctx context.Context, client microvmv1alpha1.MicroVMClient) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := client.GetMicroVM(timeoutCtx, &microvmv1alpha1.GetMicroVMRequest{Uid: "vm-1"})
	return err
}

func TestNew_Insecure_AcceptsPlaintext(t *testing.T) {
	addr := startServer(t, config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{Insecure: true}})
	client := dial(t, addr, insecure.NewCredentials())

	if err := callGetMicroVM(context.Background(), client); err != nil {
		t.Fatalf("GetMicroVM: %v", err)
	}
}

func TestNew_TLS_AcceptsMatchingCA(t *testing.T) {
	certPath, keyPath := genSelfSignedCert(t)
	addr := startServer(t, config.APIServerConfig{
		Addr: ":0",
		TLS:  config.ServerTLSConfig{CertFile: certPath, KeyFile: keyPath},
	})

	clientCreds := tlsClientCreds(t, certPath, nil)
	client := dial(t, addr, clientCreds)

	if err := callGetMicroVM(context.Background(), client); err != nil {
		t.Fatalf("GetMicroVM: %v", err)
	}
}

func TestNew_TLS_RejectsPlaintext(t *testing.T) {
	certPath, keyPath := genSelfSignedCert(t)
	addr := startServer(t, config.APIServerConfig{
		Addr: ":0",
		TLS:  config.ServerTLSConfig{CertFile: certPath, KeyFile: keyPath},
	})

	client := dial(t, addr, insecure.NewCredentials())

	if err := callGetMicroVM(context.Background(), client); err == nil {
		t.Fatalf("expected error dialing TLS server without TLS, got nil")
	}
}

func TestNew_MTLS_AcceptsValidClientCert(t *testing.T) {
	serverCertPath, serverKeyPath := genSelfSignedCert(t)
	clientCertPath, clientKeyPath := genSelfSignedCert(t)

	addr := startServer(t, config.APIServerConfig{
		Addr: ":0",
		TLS: config.ServerTLSConfig{
			CertFile: serverCertPath, KeyFile: serverKeyPath,
			ValidateClient: true, ClientCAFile: clientCertPath,
		},
	})

	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		t.Fatalf("load client cert: %v", err)
	}
	clientCreds := tlsClientCreds(t, serverCertPath, &clientCert)
	client := dial(t, addr, clientCreds)

	if err := callGetMicroVM(context.Background(), client); err != nil {
		t.Fatalf("GetMicroVM: %v", err)
	}
}

func TestNew_MTLS_RejectsMissingClientCert(t *testing.T) {
	serverCertPath, serverKeyPath := genSelfSignedCert(t)
	clientCACertPath, _ := genSelfSignedCert(t)

	addr := startServer(t, config.APIServerConfig{
		Addr: ":0",
		TLS: config.ServerTLSConfig{
			CertFile: serverCertPath, KeyFile: serverKeyPath,
			ValidateClient: true, ClientCAFile: clientCACertPath,
		},
	})

	clientCreds := tlsClientCreds(t, serverCertPath, nil)
	client := dial(t, addr, clientCreds)

	if err := callGetMicroVM(context.Background(), client); err == nil {
		t.Fatalf("expected error dialing mTLS server without a client cert, got nil")
	}
}

func TestNew_BasicAuth_AcceptsCorrectToken(t *testing.T) {
	addr := startServer(t, config.APIServerConfig{
		Addr:           ":0",
		TLS:            config.ServerTLSConfig{Insecure: true},
		BasicAuthToken: "s3cret",
	})
	client := dial(t, addr, insecure.NewCredentials())

	ctx := withBasicAuth(context.Background(), "s3cret")
	if err := callGetMicroVM(ctx, client); err != nil {
		t.Fatalf("GetMicroVM: %v", err)
	}
}

func TestNew_BasicAuth_RejectsWrongToken(t *testing.T) {
	addr := startServer(t, config.APIServerConfig{
		Addr:           ":0",
		TLS:            config.ServerTLSConfig{Insecure: true},
		BasicAuthToken: "s3cret",
	})
	client := dial(t, addr, insecure.NewCredentials())

	ctx := withBasicAuth(context.Background(), "wrong")
	if err := callGetMicroVM(ctx, client); err == nil {
		t.Fatalf("expected error for wrong basic auth token, got nil")
	}
}

func TestNew_BasicAuth_RejectsMissingToken(t *testing.T) {
	addr := startServer(t, config.APIServerConfig{
		Addr:           ":0",
		TLS:            config.ServerTLSConfig{Insecure: true},
		BasicAuthToken: "s3cret",
	})
	client := dial(t, addr, insecure.NewCredentials())

	if err := callGetMicroVM(context.Background(), client); err == nil {
		t.Fatalf("expected error for missing basic auth token, got nil")
	}
}

func TestNew_InvalidServerCertPath_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	cfg := config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{
		CertFile: filepath.Join(dir, "missing-cert.pem"),
		KeyFile:  filepath.Join(dir, "missing-key.pem"),
	}}

	if _, err := server.New(cfg); err == nil {
		t.Fatalf("expected error for missing cert/key files, got nil")
	}
}

func TestNew_InvalidClientCAPath_ReturnsError(t *testing.T) {
	certPath, keyPath := genSelfSignedCert(t)
	cfg := config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{
		CertFile: certPath, KeyFile: keyPath,
		ValidateClient: true, ClientCAFile: filepath.Join(t.TempDir(), "missing-ca.pem"),
	}}

	if _, err := server.New(cfg); err == nil {
		t.Fatalf("expected error for missing client CA file, got nil")
	}
}

func TestNew_ExtraServerOptions_Applied(t *testing.T) {
	var called bool
	extra := grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		called = true
		return handler(ctx, req)
	})

	srv, err := server.New(config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{Insecure: true}}, extra)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	microvmv1alpha1.RegisterMicroVMServer(srv, &fakeMicroVMServer{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	client := dial(t, lis.Addr().String(), insecure.NewCredentials())
	if err := callGetMicroVM(context.Background(), client); err != nil {
		t.Fatalf("GetMicroVM: %v", err)
	}
	if !called {
		t.Fatalf("expected extra server option's interceptor to be invoked")
	}
}

func TestNew_InvalidConfig_ReturnsError(t *testing.T) {
	// Insecure with a stray cert file is invalid per config.ServerTLSConfig.Validate.
	cfg := config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{Insecure: true, CertFile: "cert.pem"}}

	if _, err := server.New(cfg); err == nil {
		t.Fatalf("expected error for invalid config, got nil")
	}
}

// tlsClientCreds builds client transport credentials trusting serverCAPath
// as the root CA, optionally presenting clientCert for mTLS.
func tlsClientCreds(t *testing.T, serverCAPath string, clientCert *tls.Certificate) credentials.TransportCredentials {
	t.Helper()

	caPEM, err := os.ReadFile(serverCAPath)
	if err != nil {
		t.Fatalf("read server ca: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatalf("no certs found in %s", serverCAPath)
	}

	tlsConfig := &tls.Config{RootCAs: pool}
	if clientCert != nil {
		tlsConfig.Certificates = []tls.Certificate{*clientCert}
	}

	return credentials.NewTLS(tlsConfig)
}

func withBasicAuth(ctx context.Context, token string) context.Context {
	encoded := base64.StdEncoding.EncodeToString([]byte(token))
	return metadata.AppendToOutgoingContext(ctx, "authorization", "basic "+encoded)
}
