package main

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/poolmanager"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func openTestStore(t *testing.T) store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "poolmgr.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func newTestPoolManager(t *testing.T, st store.Store) *poolmanager.Manager {
	t.Helper()
	return poolmanager.New(context.Background(), st, nil, metrics.NewRegistry())
}

// dialInsecure dials addr with insecure transport credentials and returns
// the connection, closed automatically at the end of the test.
func dialInsecure(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestBuildGRPCServer_RegistersApplicationServices(t *testing.T) {
	st := openTestStore(t)
	reg := metrics.NewRegistry()
	cfg := config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{Insecure: true}}
	poolMgr := newTestPoolManager(t, st)

	srv, err := buildGRPCServer(cfg, st, nil, reg, poolMgr)
	if err != nil {
		t.Fatalf("buildGRPCServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn := dialInsecure(t, lis.Addr().String())

	if _, err := poolmgrv1alpha1.NewPoolAdminClient(conn).ListPools(context.Background(), &poolmgrv1alpha1.ListPoolsRequest{}); err != nil {
		t.Fatalf("ListPools: %v", err)
	}
}

func TestBuildGRPCServer_RegistersHealthService(t *testing.T) {
	st := openTestStore(t)
	reg := metrics.NewRegistry()
	cfg := config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{Insecure: true}}
	poolMgr := newTestPoolManager(t, st)

	srv, err := buildGRPCServer(cfg, st, nil, reg, poolMgr)
	if err != nil {
		t.Fatalf("buildGRPCServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn := dialInsecure(t, lis.Addr().String())

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health Check: %v", err)
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("expected SERVING, got %v", resp.Status)
	}
}

func TestBuildGRPCServer_RegistersReflection(t *testing.T) {
	st := openTestStore(t)
	reg := metrics.NewRegistry()
	cfg := config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{Insecure: true}}
	poolMgr := newTestPoolManager(t, st)

	srv, err := buildGRPCServer(cfg, st, nil, reg, poolMgr)
	if err != nil {
		t.Fatalf("buildGRPCServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn := dialInsecure(t, lis.Addr().String())
	stream, err := grpc_reflection_v1.NewServerReflectionClient(conn).ServerReflectionInfo(context.Background())
	if err != nil {
		t.Fatalf("ServerReflectionInfo: %v", err)
	}
	if err := stream.Send(&grpc_reflection_v1.ServerReflectionRequest{MessageRequest: &grpc_reflection_v1.ServerReflectionRequest_ListServices{}}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("recv: %v", err)
	}
}

func TestServeGRPC_ShutsDownWithActiveStream(t *testing.T) {
	st := openTestStore(t)
	reg := metrics.NewRegistry()
	cfg := config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{Insecure: true}}
	poolMgr := newTestPoolManager(t, st)

	srv, err := buildGRPCServer(cfg, st, nil, reg, poolMgr)
	if err != nil {
		t.Fatalf("buildGRPCServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveGRPC(runCtx, srv, lis) }()

	conn := dialInsecure(t, lis.Addr().String())
	watchStream, err := grpc_health_v1.NewHealthClient(conn).Watch(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if _, err := watchStream.Recv(); err != nil {
		t.Fatalf("Watch Recv (initial status): %v", err)
	}

	// The Watch stream above is still open (its own context is independent
	// of runCtx). Cancelling runCtx must still make serveGRPC return
	// promptly instead of hanging on GracefulStop waiting for the stream.
	cancel()

	select {
	case <-done:
	case <-time.After(grpcShutdownTimeout + 2*time.Second):
		t.Fatalf("serveGRPC did not return within grpcShutdownTimeout of an active stream blocking GracefulStop")
	}
}

// TestSweeperRun_ShutsDownOnContextCancel exercises the Sweeper.Run contract
// exactly as main() relies on it: run in a goroutine reporting into an
// errCh, cancel runCtx, and confirm it returns promptly with
// context.Canceled so the drain loop in main() treats it as a clean exit.
func TestSweeperRun_ShutsDownOnContextCancel(t *testing.T) {
	st := openTestStore(t)
	reg := metrics.NewRegistry()
	poolMgr := newTestPoolManager(t, st)
	sweeper := reconciler.NewSweeper(st, nil, time.Millisecond, 0, poolMgr, reg)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sweeper.Run(runCtx) }()

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("sweeper.Run did not return promptly after context cancellation")
	}
}

func TestBuildGRPCServer_InvalidConfig_ReturnsError(t *testing.T) {
	st := openTestStore(t)
	reg := metrics.NewRegistry()
	cfg := config.APIServerConfig{TLS: config.ServerTLSConfig{Insecure: true, CertFile: "cert.pem"}}
	poolMgr := newTestPoolManager(t, st)

	if _, err := buildGRPCServer(cfg, st, nil, reg, poolMgr); err == nil {
		t.Fatalf("expected error for invalid config, got nil")
	}
}
