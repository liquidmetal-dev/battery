package grpcconfig_test

import (
	"testing"

	"github.com/liquidmetal-dev/battery/internal/grpcconfig"
)

func TestLoadTLSCredentialsWithCertAndKey(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateSelfSignedCert(t, dir, "server", false)

	creds, err := grpcconfig.LoadTLSCredentials(grpcconfig.TLSConfig{
		CertFile: certPath,
		KeyFile:  keyPath,
	})
	if err != nil {
		t.Fatalf("LoadTLSCredentials returned error: %v", err)
	}
	if creds == nil {
		t.Fatal("expected non-nil credentials")
	}
	if got := creds.Info().SecurityProtocol; got != "tls" {
		t.Fatalf("expected tls security protocol, got %q", got)
	}
}

func TestLoadTLSCredentialsWithClientValidation(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateSelfSignedCert(t, dir, "server", false)
	caPath, _ := generateSelfSignedCert(t, dir, "ca", true)

	creds, err := grpcconfig.LoadTLSCredentials(grpcconfig.TLSConfig{
		CertFile:       certPath,
		KeyFile:        keyPath,
		ValidateClient: true,
		ClientCAFile:   caPath,
	})
	if err != nil {
		t.Fatalf("LoadTLSCredentials returned error: %v", err)
	}
	if creds == nil {
		t.Fatal("expected non-nil credentials")
	}
}

func TestLoadTLSCredentialsMissingCertFile(t *testing.T) {
	dir := t.TempDir()
	_, keyPath := generateSelfSignedCert(t, dir, "server", false)

	_, err := grpcconfig.LoadTLSCredentials(grpcconfig.TLSConfig{
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  keyPath,
	})
	if err == nil {
		t.Fatal("expected an error for a missing cert file")
	}
}

func TestLoadTLSCredentialsMissingClientCAFile(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateSelfSignedCert(t, dir, "server", false)

	_, err := grpcconfig.LoadTLSCredentials(grpcconfig.TLSConfig{
		CertFile:       certPath,
		KeyFile:        keyPath,
		ValidateClient: true,
		ClientCAFile:   "/nonexistent/ca.pem",
	})
	if err == nil {
		t.Fatal("expected an error for a missing client CA file")
	}
}
