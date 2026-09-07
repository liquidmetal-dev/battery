// Command poolmgrd is the MicroVM Warm Pool Manager server entrypoint.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func main() {
	configPath := flag.String("config", "", "path to the pool manager's JSON config file")
	dbPath := flag.String("db", "poolmgr.db", "path to the pool manager's SQLite database")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	metricsAddr := config.DefaultMetricsAddr
	if *configPath != "" {
		cfg, err := config.Load(*configPath)
		if err != nil {
			log.Fatalf("poolmgrd: %v", err)
		}
		metricsAddr = cfg.MetricsAddr
	}

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("poolmgrd: open store: %v", err)
	}
	defer st.Close()

	reg := metrics.NewRegistry()
	reg.RegisterPoolCollector(st)

	if err := serveMetrics(ctx, metricsAddr, reg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("poolmgrd: %v", err)
	}

	log.Println("poolmgrd: not yet implemented (gRPC server bootstrap is a separate issue)")
}

// serveMetrics runs the /metrics HTTP listener until ctx is done, then
// shuts it down gracefully. This is the only HTTP/gRPC surface poolmgrd
// currently stands up - the gRPC API server bootstrap (grpc.NewServer,
// interceptors, service registration) is out of scope until a later issue,
// at which point it should call metrics.Registry's ServerOptions/
// RegisterGRPCServer against this same reg.
func serveMetrics(ctx context.Context, addr string, reg *metrics.Registry) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           reg.Handler(),
		ReadHeaderTimeout: 3 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("poolmgrd: /metrics listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return ctx.Err()
}
