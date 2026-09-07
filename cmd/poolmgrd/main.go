// Command poolmgrd is the MicroVM Warm Pool Manager server entrypoint.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/server"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func main() {
	configPath := flag.String("config", "", "path to the pool manager's JSON config file")
	dbPath := flag.String("db", "poolmgr.db", "path to the pool manager's SQLite database")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("poolmgrd: -config is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("poolmgrd: %v", err)
	}

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("poolmgrd: open store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Printf("poolmgrd: close store: %v", err)
		}
	}()

	flint, err := flintlockclient.New(cfg)
	if err != nil {
		log.Fatalf("poolmgrd: flintlock client pool: %v", err)
	}

	reg := metrics.NewRegistry()
	reg.RegisterPoolCollector(st)

	// runCtx is cancelled either by the outer signal-driven ctx, or by us
	// below if one server fails - either way, both listeners shut down
	// together and we drain both results before exiting.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	errCh := make(chan error, 2)
	pending := 1

	go func() {
		errCh <- serveMetrics(runCtx, cfg.MetricsAddr, reg)
	}()

	if cfg.APIServer != nil {
		grpcSrv, err := buildGRPCServer(*cfg.APIServer, st, flint, reg)
		if err != nil {
			log.Fatalf("poolmgrd: %v", err)
		}

		lis, err := net.Listen("tcp", cfg.APIServer.Addr)
		if err != nil {
			log.Fatalf("poolmgrd: listen on %s: %v", cfg.APIServer.Addr, err)
		}

		pending++
		go func() {
			errCh <- serveGRPC(runCtx, grpcSrv, lis)
		}()
	} else {
		log.Println("poolmgrd: no api_server configured, gRPC API is disabled")
	}

	var firstErr error
	for i := 0; i < pending; i++ {
		if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) && firstErr == nil {
			firstErr = err
			stop()
		}
	}
	if firstErr != nil {
		log.Fatalf("poolmgrd: %v", firstErr)
	}
}

// buildGRPCServer constructs the pool manager's gRPC server per cfg,
// registers the PoolAdmin, Lease, and Events services (backed by st and
// flint) plus grpc/health and reflection, and pre-registers reg's gRPC
// metrics. The caller still needs to net.Listen and Serve it.
func buildGRPCServer(cfg config.APIServerConfig, st store.Store, flint *flintlockclient.Pool, reg *metrics.Registry) (*grpc.Server, error) {
	srv, err := server.New(cfg, reg.ServerOptions()...)
	if err != nil {
		return nil, fmt.Errorf("build grpc server: %w", err)
	}

	poolmgrv1alpha1.RegisterPoolAdminServer(srv, api.NewPoolAdminServer(st))
	poolmgrv1alpha1.RegisterLeaseServer(srv, api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, reg))
	poolmgrv1alpha1.RegisterEventsServer(srv, api.NewEventsServer(st, 0, 0))

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(srv, healthSrv)

	reflection.Register(srv)

	reg.RegisterGRPCServer(srv)

	return srv, nil
}

// grpcShutdownTimeout bounds serveGRPC's graceful stop: a long-lived stream
// (Events.Subscribe, the health service's Watch) stays open until its
// client disconnects and would otherwise block GracefulStop indefinitely,
// since cancelling ctx does not cancel those RPCs' own contexts.
const grpcShutdownTimeout = 5 * time.Second

// serveGRPC runs grpcSrv on lis until ctx is done, then stops it: gracefully
// if that completes within grpcShutdownTimeout, otherwise it force-closes
// any still-active connections/streams via Stop.
func serveGRPC(ctx context.Context, grpcSrv *grpc.Server, lis net.Listener) error {
	errCh := make(chan error, 1)
	go func() {
		log.Printf("poolmgrd: gRPC API listening on %s", lis.Addr())
		errCh <- grpcSrv.Serve(lis)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(grpcShutdownTimeout):
		log.Printf("poolmgrd: graceful stop exceeded %s, forcing shutdown", grpcShutdownTimeout)
		grpcSrv.Stop()
		<-stopped
	}

	return ctx.Err()
}

// serveMetrics runs the /metrics HTTP listener until ctx is done, then
// shuts it down gracefully.
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
