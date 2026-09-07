package main

import (
	"testing"

	"github.com/liquidmetal-dev/battery/internal/grpcconfig"
)

func TestRootCmdBuildsConfigFromFlags(t *testing.T) {
	var gotCfg grpcconfig.Config
	var gotVsockPath string
	var gotVsockPort int
	called := false

	cmd := newRootCmd(func(cfg grpcconfig.Config, vsockConnectPath string, vsockPort int) error {
		called = true
		gotCfg = cfg
		gotVsockPath = vsockConnectPath
		gotVsockPort = vsockPort
		return nil
	})
	cmd.SetArgs([]string{
		"--insecure",
		"--listen-address", ":9091",
		"--vsock-connect-path", "/usr/local/bin/vsock-connect",
		"--vsock-port", "2000",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !called {
		t.Fatal("expected the run callback to be invoked")
	}
	if !gotCfg.TLS.Insecure {
		t.Fatal("expected TLS.Insecure to be true")
	}
	if gotCfg.ListenAddress != ":9091" {
		t.Fatalf("unexpected listen address: %q", gotCfg.ListenAddress)
	}
	if gotVsockPath != "/usr/local/bin/vsock-connect" {
		t.Fatalf("unexpected vsock-connect path: %q", gotVsockPath)
	}
	if gotVsockPort != 2000 {
		t.Fatalf("unexpected vsock port: %d", gotVsockPort)
	}
}

func TestRootCmdDefaults(t *testing.T) {
	var gotCfg grpcconfig.Config
	var gotVsockPath string
	var gotVsockPort int

	cmd := newRootCmd(func(cfg grpcconfig.Config, vsockConnectPath string, vsockPort int) error {
		gotCfg = cfg
		gotVsockPath = vsockConnectPath
		gotVsockPort = vsockPort
		return nil
	})
	cmd.SetArgs([]string{"--insecure"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if gotCfg.ListenAddress == "" {
		t.Fatal("expected a non-empty default listen address")
	}
	if gotVsockPath != "vsock-connect" {
		t.Fatalf("expected default vsock-connect-path %q, got %q", "vsock-connect", gotVsockPath)
	}
	if gotVsockPort != 1024 {
		t.Fatalf("expected default vsock port 1024, got %d", gotVsockPort)
	}
}

func TestRootCmdRejectsInvalidTLSConfig(t *testing.T) {
	called := false
	cmd := newRootCmd(func(_ grpcconfig.Config, _ string, _ int) error {
		called = true
		return nil
	})
	// Neither --insecure nor cert/key provided.
	cmd.SetArgs([]string{})

	if err := cmd.Execute(); err == nil {
		t.Fatal("expected an error for an invalid TLS configuration")
	}
	if called {
		t.Fatal("expected the run callback not to be invoked when TLS config is invalid")
	}
}
