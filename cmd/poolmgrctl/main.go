// Command poolmgrctl is a CLI client for the pool manager's gRPC API.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/liquidmetal-dev/battery/internal/poolmgrctl"
)

func main() {
	// A cancellable root context is required so long-running commands (e.g.
	// "events tail") see cmd.Context() cancelled on Ctrl-C/SIGTERM instead
	// of running forever - cobra's Execute() alone uses context.Background()
	// and never observes process signals.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := poolmgrctl.NewRootCmd().ExecuteContext(ctx); err != nil {
		os.Exit(poolmgrctl.ExitCode(err))
	}
}
