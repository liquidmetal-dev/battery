// Command poolmgrd is the MicroVM Warm Pool Manager server entrypoint.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/timestamppb"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/poolmanager"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/server"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// fatal logs err (or msg alone, if err is nil) at Error level on the default
// logger and exits 1. Unlike stdlib log.Fatal, this doesn't run deferred
// cleanup - callers past the point where st.Close() matters should prefer
// returning the error up to a single fatal call instead.
func fatal(msg string, err error) {
	if err != nil {
		slog.Error(msg, "error", err)
	} else {
		slog.Error(msg)
	}
	os.Exit(1)
}

func main() {
	configPath := flag.String("config", "", "path to the pool manager's JSON config file")
	dbPath := flag.String("db", "poolmgr.db", "path to the pool manager's SQLite database")
	logLevel := flag.String("log-level", "debug", "log verbosity: debug, info, warn, or error")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	level, err := parseLogLevel(*logLevel)
	if err != nil {
		fatal("poolmgrd: "+err.Error(), nil)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	if *configPath == "" {
		fatal("poolmgrd: -config is required", nil)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal("poolmgrd: load config", err)
	}
	slog.Info("poolmgrd: config loaded", "config_path", *configPath)

	st, err := store.Open(*dbPath)
	if err != nil {
		fatal("poolmgrd: open store", err)
	}
	slog.Info("poolmgrd: store opened", "db_path", *dbPath)
	defer func() {
		if err := st.Close(); err != nil {
			slog.Error("poolmgrd: close store", "error", err)
		}
	}()

	if err := seedHosts(ctx, st, cfg); err != nil {
		fatal("poolmgrd: seed hosts", err)
	}

	flint, err := flintlockclient.New(cfg)
	if err != nil {
		fatal("poolmgrd: flintlock client pool", err)
	}

	reg := metrics.NewRegistry()
	reg.RegisterPoolCollector(st)

	// runCtx is cancelled either by the outer signal-driven ctx, or by us
	// below if one server fails - either way, every goroutine started below
	// shuts down together and we drain all of their results before exiting.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	poolMgr := poolmanager.New(runCtx, st, flint, reg)
	if err := poolMgr.Seed(runCtx); err != nil {
		fatal("poolmgrd: seed pool manager", err)
	}
	slog.Info("poolmgrd: pool manager seeded")

	// cfg.Validate (via config.Load) already guarantees these parse cleanly.
	sweepInterval, _ := time.ParseDuration(cfg.SweepInterval)
	warningWindow, _ := time.ParseDuration(cfg.WarningWindow)
	sweeper := reconciler.NewSweeper(st, flint, sweepInterval, warningWindow, poolMgr, reg)

	errCh := make(chan error, 4)
	pending := 3

	go func() {
		errCh <- serveMetrics(runCtx, cfg.MetricsAddr, reg)
	}()
	go func() {
		errCh <- poolMgr.Run()
	}()
	go func() {
		errCh <- sweeper.Run(runCtx)
	}()

	if cfg.APIServer != nil {
		grpcSrv, err := buildGRPCServer(*cfg.APIServer, st, flint, reg, poolMgr)
		if err != nil {
			fatal("poolmgrd: build grpc server", err)
		}

		lis, err := net.Listen("tcp", cfg.APIServer.Addr)
		if err != nil {
			fatal(fmt.Sprintf("poolmgrd: listen on %s", cfg.APIServer.Addr), err)
		}

		pending++
		go func() {
			errCh <- serveGRPC(runCtx, grpcSrv, lis)
		}()
	} else {
		slog.Warn("poolmgrd: no api_server configured, gRPC API is disabled")
	}

	var firstErr error
	for i := 0; i < pending; i++ {
		if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) && firstErr == nil {
			firstErr = err
			stop()
		}
	}
	if firstErr != nil {
		fatal("poolmgrd", firstErr)
	}
}

// parseLogLevel maps a -log-level flag value onto a slog.Level.
func parseLogLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid -log-level %q: must be debug, info, warn, or error", s)
	}
}

// seedHosts upserts a hosts registry row for every host in cfg, so the
// HostAdmin API has a known-host set to validate Drain/UndrainHost calls
// against. An existing row has its address refreshed to match cfg, but its
// drain state is left untouched - see store.Store.UpsertHostIfMissing.
func seedHosts(ctx context.Context, st store.Store, cfg *config.Config) error {
	now := timestamppb.Now()
	for _, h := range cfg.Hosts {
		host := &poolmgrv1alpha1.Host{Name: h.Name, Address: h.Address, UpdatedAt: now}
		if err := st.UpsertHostIfMissing(ctx, host); err != nil {
			return fmt.Errorf("seed host %q: %w", h.Name, err)
		}
	}
	return nil
}

// buildGRPCServer constructs the pool manager's gRPC server per cfg,
// registers the PoolAdmin, Lease, Events, and HostAdmin services (backed by
// st and flint) plus grpc/health and reflection, and pre-registers reg's
// gRPC metrics. The caller still needs to net.Listen and Serve it.
func buildGRPCServer(cfg config.APIServerConfig, st store.Store, flint *flintlockclient.Pool, reg *metrics.Registry, poolMgr *poolmanager.Manager) (*grpc.Server, error) {
	srv, err := server.New(cfg, reg.ServerOptions()...)
	if err != nil {
		return nil, fmt.Errorf("build grpc server: %w", err)
	}

	poolmgrv1alpha1.RegisterPoolAdminServer(srv, api.NewPoolAdminServer(st, poolMgr))
	poolmgrv1alpha1.RegisterLeaseServer(srv, api.NewLeaseServer(st, flint, api.HookExecConfig{}, poolMgr, reg))
	poolmgrv1alpha1.RegisterEventsServer(srv, api.NewEventsServer(st, 0, 0))
	poolmgrv1alpha1.RegisterHostAdminServer(srv, api.NewHostAdminServer(st))

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
		slog.Info("poolmgrd: gRPC API listening", "addr", lis.Addr().String())
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
		slog.Info("poolmgrd: gRPC API stopped")
	case <-time.After(grpcShutdownTimeout):
		slog.Warn("poolmgrd: graceful stop exceeded timeout, forcing shutdown", "timeout", grpcShutdownTimeout)
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
		slog.Info("poolmgrd: /metrics listening", "addr", addr)
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
	slog.Info("poolmgrd: /metrics stopped")
	return ctx.Err()
}
