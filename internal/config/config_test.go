package config_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/liquidmetal-dev/battery/internal/config"
)

func writeConfigFile(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	return path
}

func TestLoad_ValidInsecureHost(t *testing.T) {
	path := writeConfigFile(t, `{
		"hosts": [
			{"name": "host-a", "address": "10.0.0.1:9090", "tls": {"insecure": true}}
		]
	}`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(cfg.Hosts) != 1 {
		t.Fatalf("expected 1 host, got %d", len(cfg.Hosts))
	}
	host := cfg.Hosts[0]
	if host.Name != "host-a" || host.Address != "10.0.0.1:9090" || !host.TLS.Insecure {
		t.Fatalf("unexpected host: %+v", host)
	}
}

func TestLoad_ValidMTLSHost(t *testing.T) {
	path := writeConfigFile(t, `{
		"hosts": [
			{"name": "host-a", "address": "10.0.0.1:9090", "tls": {
				"ca_file": "/etc/pool/ca.pem",
				"cert_file": "/etc/pool/client.pem",
				"key_file": "/etc/pool/client-key.pem"
			}}
		]
	}`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	tls := cfg.Hosts[0].TLS
	if tls.Insecure {
		t.Fatalf("expected Insecure=false")
	}
	if tls.CAFile != "/etc/pool/ca.pem" || tls.CertFile != "/etc/pool/client.pem" || tls.KeyFile != "/etc/pool/client-key.pem" {
		t.Fatalf("unexpected tls config: %+v", tls)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := config.Load(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if err == nil {
		t.Fatalf("expected error for missing file")
	}
}

func TestLoad_InvalidJSON(t *testing.T) {
	path := writeConfigFile(t, `{not valid json`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected error for invalid json")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		hosts   []config.HostConfig
		wantErr bool
	}{
		{
			name: "valid insecure",
			hosts: []config.HostConfig{
				{Name: "a", Address: "127.0.0.1:1", TLS: config.TLSConfig{Insecure: true}},
			},
			wantErr: false,
		},
		{
			name: "valid mtls",
			hosts: []config.HostConfig{
				{Name: "a", Address: "127.0.0.1:1", TLS: config.TLSConfig{CAFile: "ca.pem"}},
			},
			wantErr: false,
		},
		{
			name: "valid tls no client cert",
			hosts: []config.HostConfig{
				{Name: "a", Address: "127.0.0.1:1", TLS: config.TLSConfig{CAFile: "ca.pem"}},
			},
			wantErr: false,
		},
		{
			name:    "no hosts",
			hosts:   []config.HostConfig{},
			wantErr: true,
		},
		{
			name: "missing name",
			hosts: []config.HostConfig{
				{Address: "127.0.0.1:1", TLS: config.TLSConfig{Insecure: true}},
			},
			wantErr: true,
		},
		{
			name: "missing address",
			hosts: []config.HostConfig{
				{Name: "a", TLS: config.TLSConfig{Insecure: true}},
			},
			wantErr: true,
		},
		{
			name: "duplicate name",
			hosts: []config.HostConfig{
				{Name: "a", Address: "127.0.0.1:1", TLS: config.TLSConfig{Insecure: true}},
				{Name: "a", Address: "127.0.0.1:2", TLS: config.TLSConfig{Insecure: true}},
			},
			wantErr: true,
		},
		{
			name: "insecure with stray ca file",
			hosts: []config.HostConfig{
				{Name: "a", Address: "127.0.0.1:1", TLS: config.TLSConfig{Insecure: true, CAFile: "ca.pem"}},
			},
			wantErr: true,
		},
		{
			name: "non-insecure missing ca file",
			hosts: []config.HostConfig{
				{Name: "a", Address: "127.0.0.1:1", TLS: config.TLSConfig{}},
			},
			wantErr: true,
		},
		{
			name: "cert without key",
			hosts: []config.HostConfig{
				{Name: "a", Address: "127.0.0.1:1", TLS: config.TLSConfig{CAFile: "ca.pem", CertFile: "cert.pem"}},
			},
			wantErr: true,
		},
		{
			name: "key without cert",
			hosts: []config.HostConfig{
				{Name: "a", Address: "127.0.0.1:1", TLS: config.TLSConfig{CAFile: "ca.pem", KeyFile: "key.pem"}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{Hosts: tt.hosts}
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestLoad_InvalidConfigFailsValidation(t *testing.T) {
	path := writeConfigFile(t, `{"hosts": []}`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected error for empty host list")
	}
	if n := strings.Count(err.Error(), "config:"); n != 1 {
		t.Fatalf("expected exactly one \"config:\" prefix in error, got %d: %v", n, err)
	}
}

func TestServerTLSConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		tls     config.ServerTLSConfig
		wantErr bool
	}{
		{
			name:    "valid insecure",
			tls:     config.ServerTLSConfig{Insecure: true},
			wantErr: false,
		},
		{
			name:    "valid tls no client validation",
			tls:     config.ServerTLSConfig{CertFile: "cert.pem", KeyFile: "key.pem"},
			wantErr: false,
		},
		{
			name: "valid mtls",
			tls: config.ServerTLSConfig{
				CertFile: "cert.pem", KeyFile: "key.pem",
				ValidateClient: true, ClientCAFile: "ca.pem",
			},
			wantErr: false,
		},
		{
			name:    "insecure with stray cert file",
			tls:     config.ServerTLSConfig{Insecure: true, CertFile: "cert.pem"},
			wantErr: true,
		},
		{
			name:    "non-insecure missing cert and key",
			tls:     config.ServerTLSConfig{},
			wantErr: true,
		},
		{
			name:    "non-insecure missing key",
			tls:     config.ServerTLSConfig{CertFile: "cert.pem"},
			wantErr: true,
		},
		{
			name:    "non-insecure missing cert",
			tls:     config.ServerTLSConfig{KeyFile: "key.pem"},
			wantErr: true,
		},
		{
			name:    "validate client without ca file",
			tls:     config.ServerTLSConfig{CertFile: "cert.pem", KeyFile: "key.pem", ValidateClient: true},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tls.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestAPIServerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		apiCfg  config.APIServerConfig
		wantErr bool
	}{
		{
			name:    "valid insecure, no basic auth",
			apiCfg:  config.APIServerConfig{TLS: config.ServerTLSConfig{Insecure: true}},
			wantErr: false,
		},
		{
			name: "valid insecure with basic auth token",
			apiCfg: config.APIServerConfig{
				TLS:            config.ServerTLSConfig{Insecure: true},
				BasicAuthToken: "s3cret",
			},
			wantErr: false,
		},
		{
			name:    "invalid tls propagates",
			apiCfg:  config.APIServerConfig{TLS: config.ServerTLSConfig{}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.apiCfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestConfig_Validate_APIServer(t *testing.T) {
	validHosts := []config.HostConfig{
		{Name: "a", Address: "127.0.0.1:1", TLS: config.TLSConfig{Insecure: true}},
	}

	t.Run("nil api server is valid", func(t *testing.T) {
		cfg := &config.Config{Hosts: validHosts}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("valid api server", func(t *testing.T) {
		cfg := &config.Config{
			Hosts:     validHosts,
			APIServer: &config.APIServerConfig{TLS: config.ServerTLSConfig{Insecure: true}},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid api server fails config validation", func(t *testing.T) {
		cfg := &config.Config{
			Hosts:     validHosts,
			APIServer: &config.APIServerConfig{TLS: config.ServerTLSConfig{}},
		}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

func TestLoad_ValidAPIServerInsecure(t *testing.T) {
	path := writeConfigFile(t, `{
		"hosts": [
			{"name": "host-a", "address": "10.0.0.1:9090", "tls": {"insecure": true}}
		],
		"api_server": {"tls": {"insecure": true}}
	}`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.APIServer == nil || !cfg.APIServer.TLS.Insecure {
		t.Fatalf("unexpected api server config: %+v", cfg.APIServer)
	}
}

func TestLoad_ValidAPIServerMTLS(t *testing.T) {
	path := writeConfigFile(t, `{
		"hosts": [
			{"name": "host-a", "address": "10.0.0.1:9090", "tls": {"insecure": true}}
		],
		"api_server": {
			"tls": {
				"cert_file": "/etc/pool/server.pem",
				"key_file": "/etc/pool/server-key.pem",
				"validate_client": true,
				"client_ca_file": "/etc/pool/client-ca.pem"
			},
			"basic_auth_token": "s3cret"
		}
	}`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	tls := cfg.APIServer.TLS
	if tls.CertFile != "/etc/pool/server.pem" || tls.KeyFile != "/etc/pool/server-key.pem" ||
		!tls.ValidateClient || tls.ClientCAFile != "/etc/pool/client-ca.pem" {
		t.Fatalf("unexpected tls config: %+v", tls)
	}
	if cfg.APIServer.BasicAuthToken != "s3cret" {
		t.Fatalf("unexpected basic auth token: %q", cfg.APIServer.BasicAuthToken)
	}
}

// sanity check that our wire format round-trips as expected JSON tags.
func TestHostConfigJSONTags(t *testing.T) {
	b, err := json.Marshal(config.HostConfig{
		Name:    "a",
		Address: "127.0.0.1:1",
		TLS:     config.TLSConfig{CAFile: "ca.pem", CertFile: "cert.pem", KeyFile: "key.pem"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["name"]; !ok {
		t.Fatalf("expected lower-case json tags, got: %s", b)
	}
}

func TestAPIServerConfigJSONTags(t *testing.T) {
	b, err := json.Marshal(config.APIServerConfig{
		TLS: config.ServerTLSConfig{
			CertFile: "cert.pem", KeyFile: "key.pem",
			ValidateClient: true, ClientCAFile: "ca.pem",
		},
		BasicAuthToken: "s3cret",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["basic_auth_token"]; !ok {
		t.Fatalf("expected lower-case json tags, got: %s", b)
	}
	tlsMap, ok := m["tls"].(map[string]any)
	if !ok {
		t.Fatalf("expected tls object, got: %s", b)
	}
	if _, ok := tlsMap["validate_client"]; !ok {
		t.Fatalf("expected lower-case json tags, got: %s", b)
	}
}
