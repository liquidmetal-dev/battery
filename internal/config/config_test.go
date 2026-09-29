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

// minimalConfig is the smallest config file Load accepts.
const minimalConfig = `{"api_server": {"addr": ":8443", "tls": {"insecure": true}}}`

// validAPIServer returns an APIServerConfig that passes Validate.
func validAPIServer() *config.APIServerConfig {
	return &config.APIServerConfig{Addr: ":8443", TLS: config.ServerTLSConfig{Insecure: true}}
}

func TestLoad_Minimal(t *testing.T) {
	cfg, err := config.Load(writeConfigFile(t, minimalConfig))
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.MetricsAddr != config.DefaultMetricsAddr {
		t.Fatalf("expected default metrics_addr %q, got %q", config.DefaultMetricsAddr, cfg.MetricsAddr)
	}
}

func TestLoad_ExplicitMetricsAddr(t *testing.T) {
	path := writeConfigFile(t, `{
		"api_server": {"addr": ":8443", "tls": {"insecure": true}},
		"metrics_addr": ":9999"
	}`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.MetricsAddr != ":9999" {
		t.Fatalf("expected metrics_addr :9999, got %q", cfg.MetricsAddr)
	}
}

// TestLoad_HostsKeyRejected: a config file from before hosts moved to the
// store must fail loudly and point at the replacement, even an empty list.
func TestLoad_HostsKeyRejected(t *testing.T) {
	for name, contents := range map[string]string{
		"with hosts": `{
			"hosts": [{"name": "host-a", "address": "10.0.0.1:9090", "tls": {"insecure": true}}],
			"api_server": {"addr": ":8443", "tls": {"insecure": true}}
		}`,
		"empty hosts": `{"hosts": [], "api_server": {"addr": ":8443", "tls": {"insecure": true}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := config.Load(writeConfigFile(t, contents))
			if err == nil {
				t.Fatalf("expected error for config with a hosts key")
			}
			if !strings.Contains(err.Error(), "poolmgrctl host add") {
				t.Fatalf("expected error to name poolmgrctl host add, got: %v", err)
			}
		})
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

func TestValidate_SweepIntervalAndWarningWindow(t *testing.T) {
	tests := []struct {
		name          string
		sweepInterval string
		warningWindow string
		wantErr       bool
	}{
		{name: "both empty uses defaults", wantErr: false},
		{name: "valid durations", sweepInterval: "10s", warningWindow: "30s", wantErr: false},
		{name: "invalid sweep_interval", sweepInterval: "not-a-duration", wantErr: true},
		{name: "invalid warning_window", warningWindow: "not-a-duration", wantErr: true},
		{name: "zero sweep_interval is invalid", sweepInterval: "0s", wantErr: true},
		{name: "negative warning_window is invalid", warningWindow: "-5s", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{
				APIServer:     validAPIServer(),
				SweepInterval: tt.sweepInterval,
				WarningWindow: tt.warningWindow,
			}
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

func TestLoad_SweepIntervalAndWarningWindow(t *testing.T) {
	path := writeConfigFile(t, `{
		"api_server": {"addr": ":8443", "tls": {"insecure": true}},
		"sweep_interval": "15s",
		"warning_window": "45s"
	}`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.SweepInterval != "15s" || cfg.WarningWindow != "45s" {
		t.Fatalf("unexpected sweep config: sweep_interval=%q warning_window=%q", cfg.SweepInterval, cfg.WarningWindow)
	}
}

func TestLoad_InvalidConfigFailsValidation(t *testing.T) {
	path := writeConfigFile(t, `{"sweep_interval": "10s"}`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatalf("expected error for missing api_server")
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
			apiCfg:  config.APIServerConfig{Addr: ":8443", TLS: config.ServerTLSConfig{Insecure: true}},
			wantErr: false,
		},
		{
			name: "valid insecure with basic auth token",
			apiCfg: config.APIServerConfig{
				Addr:           ":8443",
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
		{
			name:    "missing addr is invalid",
			apiCfg:  config.APIServerConfig{Addr: "", TLS: config.ServerTLSConfig{Insecure: true}},
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
	t.Run("nil api server is invalid", func(t *testing.T) {
		cfg := &config.Config{}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected error, got nil")
		}
	})

	t.Run("valid api server", func(t *testing.T) {
		cfg := &config.Config{APIServer: validAPIServer()}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid api server fails config validation", func(t *testing.T) {
		cfg := &config.Config{
			APIServer: &config.APIServerConfig{TLS: config.ServerTLSConfig{}},
		}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

func TestLoad_ValidAPIServerInsecure(t *testing.T) {
	path := writeConfigFile(t, `{
		"api_server": {"addr": ":8443", "tls": {"insecure": true}}
	}`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.APIServer == nil || !cfg.APIServer.TLS.Insecure {
		t.Fatalf("unexpected api server config: %+v", cfg.APIServer)
	}
	if cfg.APIServer.Addr != ":8443" {
		t.Fatalf("unexpected api server addr: %q", cfg.APIServer.Addr)
	}
}

func TestLoad_ValidAPIServerMTLS(t *testing.T) {
	path := writeConfigFile(t, `{
		"api_server": {
			"addr": ":8443",
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
func TestAPIServerConfigJSONTags(t *testing.T) {
	b, err := json.Marshal(config.APIServerConfig{
		Addr: ":8443",
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
	if _, ok := m["addr"]; !ok {
		t.Fatalf("expected addr json tag, got: %s", b)
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
