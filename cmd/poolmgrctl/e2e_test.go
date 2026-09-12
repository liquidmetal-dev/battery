//go:build e2e

// e2e_test.go drives the real poolmgrctl cobra command tree
// (internal/poolmgrctl.NewRootCmd) against a real, in-process poolmgrd -
// itself built the same way cmd/poolmgrd's main() and its own
// cmd/poolmgrd/e2e_test.go do (store.Open, a fake-flintlock-backed
// flintlockclient.Pool, poolmanager.Manager, internal/server.New plus the
// PoolAdmin/Lease/Events services) - both listening on loopback ports.
//
// Unlike cmd/poolmgrd/e2e_test.go, which talks to poolmgrd via generated
// gRPC client stubs, this test goes through the actual poolmgrctl CLI: it
// constructs poolmgrctl.NewRootCmd(), sets args/output buffers, and calls
// Execute(), proving the CLI's flag parsing, dialing, and output
// formatting all work against a real server, not just against fakes/
// bufconn in internal/poolmgrctl's own unit tests.
//
// Run with:
//
//	go test -tags e2e ./... -run TestE2E -v
//
// Excluded from the default `go test ./...` by the e2e build tag above.
package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/e2etest"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/poolmanager"
	"github.com/liquidmetal-dev/battery/internal/poolmgrctl"
	"github.com/liquidmetal-dev/battery/internal/server"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// e2eTestTimeout bounds the whole test. It must comfortably exceed
// reconciler.DefaultTickInterval (10s): a freshly started reconciler only
// provisions on its first tick, not on start.
const e2eTestTimeout = 40 * time.Second

const e2ePollInterval = 200 * time.Millisecond

// grpcShutdownTimeout bounds startE2EPoolmgrd's graceful stop, mirroring
// cmd/poolmgrd/main.go's serveGRPC: a long-lived stream (Events.Subscribe)
// stays open until its client disconnects and would otherwise block
// GracefulStop indefinitely, since cancelling ctx does not cancel those
// RPCs' own contexts.
const grpcShutdownTimeout = 5 * time.Second

// startE2EPoolmgrd builds and serves a real poolmgrd - the same
// construction cmd/poolmgrd's main()/buildGRPCServer use (store, flint,
// poolmanager.Manager, internal/server.New plus the PoolAdmin/Lease/Events
// services) minus the CLI/config-file/signal-handling glue - on a loopback
// port, and returns the address poolmgrctl should dial.
func startE2EPoolmgrd(t *testing.T, flint *flintlockclient.Pool) string {
	t.Helper()

	st, err := store.Open(filepath.Join(t.TempDir(), "poolmgr.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	reg := metrics.NewRegistry()

	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	poolMgr := poolmanager.New(runCtx, st, flint, reg)
	go func() { _ = poolMgr.Run() }()

	srv, err := server.New(config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{Insecure: true}}, reg.ServerOptions()...)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	poolmgrv1alpha1.RegisterPoolAdminServer(srv, api.NewPoolAdminServer(st, poolMgr))
	poolmgrv1alpha1.RegisterLeaseServer(srv, api.NewLeaseServer(st, flint, api.HookExecConfig{}, poolMgr, reg))
	poolmgrv1alpha1.RegisterEventsServer(srv, api.NewEventsServer(st, 0, 0))

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(srv, healthSrv)

	reflection.Register(srv)
	reg.RegisterGRPCServer(srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = serveE2EGRPC(runCtx, srv, lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// serveE2EGRPC runs grpcSrv on lis until ctx is done, then stops it:
// gracefully if that completes within grpcShutdownTimeout, otherwise it
// force-closes any still-active connections/streams via Stop - mirroring
// cmd/poolmgrd/main.go's serveGRPC, which isn't reusable here since it's an
// unexported function in a different package main.
func serveE2EGRPC(ctx context.Context, grpcSrv *grpc.Server, lis net.Listener) error {
	errCh := make(chan error, 1)
	go func() { errCh <- grpcSrv.Serve(lis) }()

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
		return nil
	case <-time.After(grpcShutdownTimeout):
		grpcSrv.Stop()
		return nil
	}
}

// writeE2ESpecFile protojson-marshals spec to a temp file and returns its
// path, for use as poolmgrctl's --spec-file.
func writeE2ESpecFile(t *testing.T, spec *poolmgrv1alpha1.PoolSpec) string {
	t.Helper()

	data, err := protojson.Marshal(spec)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}

	path := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write spec file: %v", err)
	}
	return path
}

// runCLI builds a fresh poolmgrctl root command, runs it with args plus
// the standard --addr/--insecure connection flags, and returns its
// combined stdout/stderr and any error. On failure the caller should
// t.Logf the returned output for debuggability.
func runCLI(ctx context.Context, addr string, args ...string) (string, error) {
	cmd := poolmgrctl.NewRootCmd()

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetContext(ctx)

	fullArgs := append(append([]string{}, args...), "--addr", addr, "--insecure")
	cmd.SetArgs(fullArgs)

	err := cmd.Execute()
	return out.String(), err
}

// TestE2E_PoolmgrctlLifecycle drives poolmgrctl's full command tree - pool
// create/list/get, lease claim/list/release, and a bounded events tail -
// against a real, in-process poolmgrd, proving the CLI works end-to-end
// against the real gRPC server rather than against internal/poolmgrctl's
// own bufconn-backed fakes.
func TestE2E_PoolmgrctlLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), e2eTestTimeout)
	defer cancel()

	flint := e2etest.StartFakeFlintlock(t)
	addr := startE2EPoolmgrd(t, flint)

	const (
		poolName = "e2e-pool"
		poolNS   = "e2e"
	)

	spec := e2etest.MinSizeThresholdPoolSpec(poolName, 1, 1)
	specPath := writeE2ESpecFile(t, spec)

	// Step 1: pool create.
	out, err := runCLI(ctx, addr, "pool", "create", "--spec-file", specPath)
	if err != nil {
		t.Fatalf("pool create: %v\noutput:\n%s", err, out)
	}
	if !strings.Contains(out, poolName) {
		t.Fatalf("pool create output missing pool name %q, got:\n%s", poolName, out)
	}
	t.Logf("pool create output:\n%s", out)

	// Step 2: pool list should show the created pool.
	out, err = runCLI(ctx, addr, "pool", "list")
	if err != nil {
		t.Fatalf("pool list: %v\noutput:\n%s", err, out)
	}
	if !strings.Contains(out, poolName) {
		t.Fatalf("pool list output missing pool name %q, got:\n%s", poolName, out)
	}
	t.Logf("pool list output:\n%s", out)

	// Step 3: pool get -o json should round-trip into a Pool matching what
	// was created.
	out, err = runCLI(ctx, addr, "pool", "get", "--name", poolName, "--namespace", poolNS, "-o", "json")
	if err != nil {
		t.Fatalf("pool get: %v\noutput:\n%s", err, out)
	}
	gotPool := &poolmgrv1alpha1.Pool{}
	if err := protojson.Unmarshal([]byte(out), gotPool); err != nil {
		t.Fatalf("pool get -o json: unmarshal: %v\noutput:\n%s", err, out)
	}
	if gotPool.GetSpec().GetName() != poolName || gotPool.GetSpec().GetNamespace() != poolNS {
		t.Fatalf("pool get -o json: spec.name/namespace = %q/%q, want %q/%q\noutput:\n%s",
			gotPool.GetSpec().GetName(), gotPool.GetSpec().GetNamespace(), poolName, poolNS, out)
	}

	// Wait for the pool to reach available=1 before claiming - the
	// reconciler only provisions on its first tick, so this can take up to
	// DefaultTickInterval. cmd/poolmgrd/e2e_test.go polls GetPool for this;
	// here we poll via "pool get -o json" through the CLI itself.
	waitForAvailableViaCLI(ctx, t, addr, poolName, poolNS, 1)

	// Step 4: lease claim - capture the lease_id from the table output
	// (printClaim always renders "LEASE_ID\t<id>" as its first line; "lease
	// claim" also supports -o json, but table output is a fine smoke test
	// here too).
	out, err = runCLI(ctx, addr, "lease", "claim", "--pool", poolName, "--namespace", poolNS)
	if err != nil {
		t.Fatalf("lease claim: %v\noutput:\n%s", err, out)
	}
	t.Logf("lease claim output:\n%s", out)
	leaseID := parseLeaseIDFromClaimOutput(t, out)
	if leaseID == "" {
		t.Fatalf("lease claim: could not parse lease_id from output:\n%s", out)
	}

	// Step 5: lease list should show the claimed lease.
	out, err = runCLI(ctx, addr, "lease", "list")
	if err != nil {
		t.Fatalf("lease list: %v\noutput:\n%s", err, out)
	}
	if !strings.Contains(out, leaseID) {
		t.Fatalf("lease list output missing lease_id %q, got:\n%s", leaseID, out)
	}
	t.Logf("lease list (with lease) output:\n%s", out)

	// Step 6: lease release.
	out, err = runCLI(ctx, addr, "lease", "release", "--lease-id", leaseID)
	if err != nil {
		t.Fatalf("lease release: %v\noutput:\n%s", err, out)
	}
	t.Logf("lease release output:\n%s", out)

	// Step 7: lease list should no longer show the released lease.
	out, err = runCLI(ctx, addr, "lease", "list")
	if err != nil {
		t.Fatalf("lease list (after release): %v\noutput:\n%s", err, out)
	}
	if strings.Contains(out, leaseID) {
		t.Fatalf("lease list output still contains released lease_id %q, got:\n%s", leaseID, out)
	}
	t.Logf("lease list (after release) output:\n%s", out)

	// Step 8: events tail smoke test, bounded by a short timeout so a
	// missing/broken stream fails the test instead of hanging.
	tailCtx, tailCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer tailCancel()

	out, err = runCLI(tailCtx, addr, "events", "tail", "--pool", poolName, "--namespace", poolNS)
	// tailEvents treats a cancelled context as a clean (nil) return, so the
	// bounded timeout above should surface as err == nil, not a failure.
	if err != nil {
		t.Fatalf("events tail: %v\noutput:\n%s", err, out)
	}
	t.Logf("events tail output:\n%s", out)

	for _, want := range []string{
		poolmgrv1alpha1.EventType_VM_CLAIMED.String(),
		poolmgrv1alpha1.EventType_VM_DELETED_ON_RELEASE.String(),
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("events tail output missing event type %q, got:\n%s", want, out)
		}
	}
}

// waitForAvailableViaCLI polls "pool get -o json" through the real CLI
// until the pool's available count reaches want, or ctx expires - mirroring
// cmd/poolmgrd/e2e_test.go's waitForAvailable, but driven through
// poolmgrctl instead of a raw gRPC client.
func waitForAvailableViaCLI(ctx context.Context, t *testing.T, addr, name, namespace string, want int32) {
	t.Helper()

	ticker := time.NewTicker(e2ePollInterval)
	defer ticker.Stop()

	var lastOut string
	for {
		out, err := runCLI(ctx, addr, "pool", "get", "--name", name, "--namespace", namespace, "-o", "json")
		if err != nil {
			t.Fatalf("pool get (while polling for available=%d): %v\noutput:\n%s", want, err, out)
		}
		lastOut = out

		pool := &poolmgrv1alpha1.Pool{}
		if err := protojson.Unmarshal([]byte(out), pool); err != nil {
			t.Fatalf("pool get -o json: unmarshal: %v\noutput:\n%s", err, out)
		}
		if pool.GetStatus().GetAvailableCount() == want {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for pool %s/%s to reach available=%d (last output: %s)", namespace, name, want, lastOut)
		case <-ticker.C:
		}
	}
}

// parseLeaseIDFromClaimOutput extracts the lease_id from "lease claim"'s
// table output, whose first line printClaimTable renders as
// "LEASE_ID\t<id>" (padded/aligned by tabwriter).
func parseLeaseIDFromClaimOutput(t *testing.T, out string) string {
	t.Helper()

	lines := strings.Split(out, "\n")
	fields := strings.Fields(lines[0])
	if len(fields) != 2 || fields[0] != "LEASE_ID" {
		t.Fatalf("unexpected lease claim output first line %q", lines[0])
	}
	return fields[1]
}
