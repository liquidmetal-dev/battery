package grpcconfig

import "errors"

// Validate checks that the TLS configuration is internally consistent: insecure mode must not
// carry cert/key material, non-insecure mode must have both, and client-certificate validation
// requires a CA file to validate against.
func (t TLSConfig) Validate() error {
	if t.Insecure {
		if t.CertFile != "" || t.KeyFile != "" {
			return errors.New("tls: cert/key must not be set when insecure mode is enabled")
		}
		if t.ValidateClient || t.ClientCAFile != "" {
			return errors.New("tls: client validation options must not be set when insecure mode is enabled")
		}
		return nil
	}

	if t.CertFile == "" || t.KeyFile == "" {
		return errors.New("tls: cert and key files are required unless insecure mode is enabled")
	}

	if t.ValidateClient && t.ClientCAFile == "" {
		return errors.New("tls: client CA file is required when client validation is enabled")
	}

	return nil
}
