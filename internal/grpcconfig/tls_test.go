package grpcconfig_test

import (
	"testing"

	"github.com/liquidmetal-dev/battery/internal/grpcconfig"
)

func TestTLSConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		tls     grpcconfig.TLSConfig
		wantErr bool
	}{
		{
			name: "insecure with no cert/key is valid",
			tls:  grpcconfig.TLSConfig{Insecure: true},
		},
		{
			name:    "insecure with cert set is invalid",
			tls:     grpcconfig.TLSConfig{Insecure: true, CertFile: "cert.pem"},
			wantErr: true,
		},
		{
			name:    "insecure with key set is invalid",
			tls:     grpcconfig.TLSConfig{Insecure: true, KeyFile: "key.pem"},
			wantErr: true,
		},
		{
			name:    "non-insecure missing cert and key is invalid",
			tls:     grpcconfig.TLSConfig{},
			wantErr: true,
		},
		{
			name:    "non-insecure missing key is invalid",
			tls:     grpcconfig.TLSConfig{CertFile: "cert.pem"},
			wantErr: true,
		},
		{
			name: "non-insecure with cert and key is valid",
			tls:  grpcconfig.TLSConfig{CertFile: "cert.pem", KeyFile: "key.pem"},
		},
		{
			name: "client validation with CA file is valid",
			tls: grpcconfig.TLSConfig{
				CertFile:       "cert.pem",
				KeyFile:        "key.pem",
				ValidateClient: true,
				ClientCAFile:   "ca.pem",
			},
		},
		{
			name: "client validation without CA file is invalid",
			tls: grpcconfig.TLSConfig{
				CertFile:       "cert.pem",
				KeyFile:        "key.pem",
				ValidateClient: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tls.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("expected an error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
