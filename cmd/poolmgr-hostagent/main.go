// Command poolmgr-hostagent is the per-flintlock-host sidecar entrypoint. It runs a gRPC server
// that proxies guest-agent vsock ping/exec calls (via the vsock-connect CLI) on behalf of the
// central pool manager, which has no direct filesystem access to other hosts' vsock sockets.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/grpcconfig"
	"github.com/liquidmetal-dev/battery/internal/hostagent"
	"github.com/liquidmetal-dev/battery/internal/vsockconnect"
)

func main() {
	if err := newRootCmd(run).Execute(); err != nil {
		log.Fatal(err)
	}
}

func run(cfg grpcconfig.Config, vsockConnectPath string, vsockPort int) error {
	var opts []grpc.ServerOption
	if !cfg.TLS.Insecure {
		creds, err := grpcconfig.LoadTLSCredentials(cfg.TLS)
		if err != nil {
			return fmt.Errorf("loading TLS credentials: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	server := grpc.NewServer(opts...)
	runner := vsockconnect.New(vsockConnectPath)
	poolmgrv1alpha1.RegisterHostagentServer(server, hostagent.NewServer(runner, vsockPort, 0))

	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", cfg.ListenAddress, err)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("poolmgr-hostagent listening on %s (insecure=%v)", cfg.ListenAddress, cfg.TLS.Insecure)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		log.Println("shutting down")
		server.GracefulStop()
		return nil
	}
}
