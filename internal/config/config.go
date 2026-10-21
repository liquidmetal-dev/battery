// Package config loads the static process configuration for the pool
// manager: its API server, its /metrics address, and the lease sweeper's
// timings. Flintlock hosts are not configured here; they live in the store
// and are managed through the HostAdmin API (poolmgrctl host add).
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// DefaultMetricsAddr is the address the pool manager's /metrics HTTP
// listener binds to when Config.MetricsAddr is empty.
const DefaultMetricsAddr = ":9090"

// errHostsKey is returned by Load for a config file that still carries the
// "hosts" list older releases read flintlock hosts from.
var errHostsKey = errors.New(`config: "hosts" is no longer supported: register each flintlock host with "poolmgrctl host add" instead`)

// Config is the top-level configuration: the pool manager's own API server
// config, and the address its /metrics HTTP endpoint listens on. APIServer
// is a pointer so that a config file that omits it parses as "not
// configured", which Validate then rejects, rather than as an all-zero
// value with a less helpful error.
type Config struct {
	APIServer *APIServerConfig `json:"api_server,omitempty"`
	// MetricsAddr is the address (host:port, or :port) the /metrics HTTP
	// endpoint listens on. Empty uses DefaultMetricsAddr.
	MetricsAddr string `json:"metrics_addr,omitempty"`
	// SweepInterval is how often the lease sweeper scans for expired
	// leases, as a Go duration string (e.g. "10s"). Empty uses
	// reconciler.DefaultSweepInterval.
	SweepInterval string `json:"sweep_interval,omitempty"`
	// WarningWindow is how far ahead of expiry the sweeper emits a
	// VM_EXPIRING_SOON warning, as a Go duration string (e.g. "30s"). Empty
	// uses reconciler.DefaultWarningWindow.
	WarningWindow string `json:"warning_window,omitempty"`
}

// Load reads and parses the JSON config file at path, then validates it.
// A file with a "hosts" key fails with an error naming poolmgrctl host add:
// hosts moved to the store, and silently ignoring the list would leave an
// upgraded manager with no hosts and no hint why.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	var keys map[string]json.RawMessage
	if err := json.Unmarshal(data, &keys); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if _, ok := keys["hosts"]; ok {
		return nil, fmt.Errorf("%s: %w", path, errHostsKey)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if cfg.MetricsAddr == "" {
		cfg.MetricsAddr = DefaultMetricsAddr
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return &cfg, nil
}

// Validate checks that the config has a valid API server, without which
// the manager could never be told about a flintlock host, and that the
// sweeper durations, if set, are positive.
func (c *Config) Validate() error {
	if c.APIServer == nil {
		return errors.New("config: api_server is required")
	}
	if err := c.APIServer.Validate(); err != nil {
		return fmt.Errorf("config: api_server: %w", err)
	}

	if err := validatePositiveDuration("sweep_interval", c.SweepInterval); err != nil {
		return err
	}
	if err := validatePositiveDuration("warning_window", c.WarningWindow); err != nil {
		return err
	}

	return nil
}

// validatePositiveDuration checks that s, if non-empty, parses as a positive
// Go duration. An empty s is valid (the caller falls back to a default).
func validatePositiveDuration(field, s string) error {
	if s == "" {
		return nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("config: %s: %w", field, err)
	}
	if d <= 0 {
		return fmt.Errorf("config: %s: must be positive", field)
	}
	return nil
}

// APIServerConfig controls the pool manager's own gRPC API server: its TLS
// mode and an optional basic-auth token layered on top.
type APIServerConfig struct {
	// Addr is the address (host:port, or :port) the gRPC server listens on.
	Addr           string          `json:"addr"`
	TLS            ServerTLSConfig `json:"tls"`
	BasicAuthToken string          `json:"basic_auth_token,omitempty"`
}

// Validate checks that the API server config is internally consistent.
// BasicAuthToken has no format requirements: any non-empty value enables
// basic auth, and an empty value leaves it disabled, independent of TLS mode.
func (a APIServerConfig) Validate() error {
	if a.Addr == "" {
		return errors.New("api_server: addr is required")
	}
	if err := a.TLS.Validate(); err != nil {
		return err
	}
	return nil
}

// ServerTLSConfig controls how the pool manager's API server presents
// itself: either an explicit insecure (no-TLS) mode, or TLS presenting a
// server certificate (CertFile/KeyFile, always required when not insecure),
// optionally validating client certificates (mTLS) via ValidateClient plus
// ClientCAFile. Unlike a flintlock host's ca_file, ValidateClient is a
// separate, explicit opt-in from ClientCAFile's presence: setting a CA file
// alone does not turn on client-certificate verification.
type ServerTLSConfig struct {
	Insecure       bool   `json:"insecure,omitempty"`
	CertFile       string `json:"cert_file,omitempty"`
	KeyFile        string `json:"key_file,omitempty"`
	ValidateClient bool   `json:"validate_client,omitempty"`
	ClientCAFile   string `json:"client_ca_file,omitempty"`
}

// Validate checks that the server TLS config is internally consistent: an
// insecure config carries no other TLS fields, a non-insecure config always
// has a cert/key pair, and ValidateClient requires a ClientCAFile.
func (t ServerTLSConfig) Validate() error {
	if t.Insecure {
		if t.CertFile != "" || t.KeyFile != "" || t.ValidateClient || t.ClientCAFile != "" {
			return errors.New("tls: insecure servers must not set cert_file/key_file/validate_client/client_ca_file")
		}
		return nil
	}

	if t.CertFile == "" || t.KeyFile == "" {
		return errors.New("tls: cert_file and key_file are required unless insecure is set")
	}

	if t.ValidateClient && t.ClientCAFile == "" {
		return errors.New("tls: client_ca_file is required when validate_client is set")
	}

	return nil
}
