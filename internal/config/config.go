// Package config loads the static process configuration for the pool
// manager's flintlock client pool: the list of flintlock hosts it may talk
// to, each with its address and TLS materials.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Config is the top-level configuration: the set of flintlock hosts the pool
// manager can dial.
type Config struct {
	Hosts []HostConfig `json:"hosts"`
}

// HostConfig describes one flintlock host: an address plus per-host TLS
// materials. Name is the identifier referenced by PoolSpec.FlintlockHosts.
type HostConfig struct {
	Name    string    `json:"name"`
	Address string    `json:"address"`
	TLS     TLSConfig `json:"tls"`
}

// TLSConfig controls how the pool manager connects to a flintlock host:
// either an explicit insecure (no-TLS) mode, or TLS verifying the server via
// CAFile, optionally presenting a client certificate (mTLS) via
// CertFile/KeyFile.
type TLSConfig struct {
	Insecure bool   `json:"insecure,omitempty"`
	CertFile string `json:"cert_file,omitempty"`
	KeyFile  string `json:"key_file,omitempty"`
	CAFile   string `json:"ca_file,omitempty"`
}

// Load reads and parses the JSON config file at path, then validates it.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}

	return &cfg, nil
}

// Validate checks that the config describes a usable set of flintlock
// hosts: at least one host, unique non-empty names, non-empty addresses,
// and a consistent per-host TLS configuration.
func (c *Config) Validate() error {
	if len(c.Hosts) == 0 {
		return errors.New("config: at least one host is required")
	}

	seen := make(map[string]struct{}, len(c.Hosts))
	for _, h := range c.Hosts {
		if h.Name == "" {
			return errors.New("config: host name is required")
		}
		if h.Address == "" {
			return fmt.Errorf("config: host %q: address is required", h.Name)
		}
		if _, dup := seen[h.Name]; dup {
			return fmt.Errorf("config: duplicate host name %q", h.Name)
		}
		seen[h.Name] = struct{}{}

		if err := h.TLS.Validate(); err != nil {
			return fmt.Errorf("config: host %q: %w", h.Name, err)
		}
	}

	return nil
}

// Validate checks that the TLS config is internally consistent: an insecure
// config carries no other TLS fields, and a non-insecure config has a
// CAFile plus a matched cert/key pair (or neither).
func (t TLSConfig) Validate() error {
	if t.Insecure {
		if t.CertFile != "" || t.KeyFile != "" || t.CAFile != "" {
			return errors.New("tls: insecure hosts must not set cert_file/key_file/ca_file")
		}
		return nil
	}

	if t.CAFile == "" {
		return errors.New("tls: ca_file is required unless insecure is set")
	}

	if (t.CertFile == "") != (t.KeyFile == "") {
		return errors.New("tls: cert_file and key_file must be set together")
	}

	return nil
}
