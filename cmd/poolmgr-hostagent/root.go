package main

import (
	"github.com/spf13/cobra"

	"github.com/liquidmetal-dev/battery/internal/grpcconfig"
)

// runFunc starts the poolmgr-hostagent server; extracted as a parameter of newRootCmd so tests
// can observe the config built from flags without actually starting a server.
type runFunc func(cfg grpcconfig.Config, vsockConnectPath string, vsockPort int) error

func newRootCmd(run runFunc) *cobra.Command {
	var (
		listenAddress    string
		insecure         bool
		certFile         string
		keyFile          string
		validateClient   bool
		clientCAFile     string
		vsockConnectPath string
		vsockPort        int
	)

	cmd := &cobra.Command{
		Use:           "poolmgr-hostagent",
		Short:         "Per-flintlock-host sidecar that proxies guest-agent vsock calls over gRPC",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg := grpcconfig.Config{
				ListenAddress: listenAddress,
				TLS: grpcconfig.TLSConfig{
					Insecure:       insecure,
					CertFile:       certFile,
					KeyFile:        keyFile,
					ValidateClient: validateClient,
					ClientCAFile:   clientCAFile,
				},
			}
			if err := cfg.TLS.Validate(); err != nil {
				return err
			}
			return run(cfg, vsockConnectPath, vsockPort)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&listenAddress, "listen-address", ":9091", "address for the gRPC server to listen on")
	flags.BoolVar(&insecure, "insecure", false, "disable TLS on the gRPC server")
	flags.StringVar(&certFile, "tls-cert", "", "path to the TLS certificate file")
	flags.StringVar(&keyFile, "tls-key", "", "path to the TLS key file")
	flags.BoolVar(&validateClient, "tls-client-validate", false, "require and verify client certificates (mTLS)")
	flags.StringVar(&clientCAFile, "tls-client-ca", "", "path to the CA file used to validate client certificates")
	flags.StringVar(&vsockConnectPath, "vsock-connect-path", "vsock-connect", "path to (or name of, if resolvable via $PATH) the vsock-connect binary")
	flags.IntVar(&vsockPort, "vsock-port", 1024, "vsock port the guest-agent listens on")

	return cmd
}
