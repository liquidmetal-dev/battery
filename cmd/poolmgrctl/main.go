// Command poolmgrctl is a CLI client for the pool manager's gRPC API.
package main

import (
	"os"

	"github.com/liquidmetal-dev/battery/internal/poolmgrctl"
)

func main() {
	if err := poolmgrctl.NewRootCmd().Execute(); err != nil {
		os.Exit(poolmgrctl.ExitCode(err))
	}
}
