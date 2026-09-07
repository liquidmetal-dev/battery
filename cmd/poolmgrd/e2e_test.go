//go:build e2e

// e2e_test.go is battery's automated end-to-end suite (issue #44), following
// the pattern of liquidmetal-dev/brigade's test/brigade_e2e_test.exs: it
// drives the full request path (gRPC client -> poolmgrd -> flintlock) as an
// ordinary automated test, against a fake flintlock double rather than real
// infrastructure.
//
// Both sides are started in-process as real, network-bound gRPC servers -
// nothing is mocked at the interface level:
//
//   - poolmgrd itself, built the same way main() does (store, flintlockclient
//     pool, poolmanager.Manager, buildGRPCServer), listening on a loopback
//     port.
//   - A fake flintlock (MicroVM + MicroVMExec), also listening on a loopback
//     port, standing in for a real flintlockd/Firecracker host.
//
// The test only holds generated gRPC client stubs dialed against poolmgrd's
// port: it exercises exactly what an external caller - or the manual
// runbook at docs/runbooks/e2e-manual-verification.md - would.
//
// docs/runbooks/e2e-manual-verification.md remains the place to verify
// MicroVMExec/MicroVMSSHProxy against a real guest OS, which can't be faked
// here.
//
// Run with:
//
//	go test -tags e2e ./... -run TestE2E -v
//
// Excluded from the default `go test ./...` (ci.yml's test job) by the
// e2e build tag above; run on demand, or via
// .github/workflows/e2e.yml (workflow_dispatch).
package main

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/poolmanager"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// e2eTestTimeout bounds each test. It must comfortably exceed
// reconciler.DefaultTickInterval (10s): a freshly started reconciler only
// provisions on its first tick, not on start.
const e2eTestTimeout = 40 * time.Second

const e2ePollInterval = 200 * time.Millisecond

// e2eFakeMicroVM is a minimal flintlock MicroVM service that reports every
// created microvm as CREATED on the very first GetMicroVM call: this suite
// is proving poolmgrd's own lifecycle logic, not flintlock's provisioning
// latency.
type e2eFakeMicroVM struct {
	microvmv1alpha1.UnimplementedMicroVMServer

	mu      sync.Mutex
	nextUID int
}

func (f *e2eFakeMicroVM) CreateMicroVM(_ context.Context, req *microvmv1alpha1.CreateMicroVMRequest) (*microvmv1alpha1.CreateMicroVMResponse, error) {
	spec, _ := proto.Clone(req.GetMicrovm()).(*flintlocktypes.MicroVMSpec)
	if spec == nil {
		spec = &flintlocktypes.MicroVMSpec{}
	}

	f.mu.Lock()
	f.nextUID++
	uid := fmt.Sprintf("e2e-vm-%d", f.nextUID)
	f.mu.Unlock()

	spec.Uid = &uid
	return &microvmv1alpha1.CreateMicroVMResponse{
		Microvm: &flintlocktypes.MicroVM{
			Spec:   spec,
			Status: &flintlocktypes.MicroVMStatus{State: flintlocktypes.MicroVMStatus_CREATED},
		},
	}, nil
}

func (f *e2eFakeMicroVM) GetMicroVM(_ context.Context, _ *microvmv1alpha1.GetMicroVMRequest) (*microvmv1alpha1.GetMicroVMResponse, error) {
	return &microvmv1alpha1.GetMicroVMResponse{
		Microvm: &flintlocktypes.MicroVM{
			Status: &flintlocktypes.MicroVMStatus{
				State: flintlocktypes.MicroVMStatus_CREATED,
				NetworkInterfaces: map[string]*flintlocktypes.NetworkInterfaceStatus{
					"eth0": {HostDeviceName: "eth0"},
				},
			},
		},
	}, nil
}

func (f *e2eFakeMicroVM) DeleteMicroVM(context.Context, *microvmv1alpha1.DeleteMicroVMRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// e2eFakeMicroVMExec is a minimal flintlock MicroVMExec service: every
// ExecCommand call succeeds immediately with exit code 0, satisfying
// flintlockclient.WaitReady's guest-agent readiness probe.
type e2eFakeMicroVMExec struct {
	microvmexecv1alpha1.UnimplementedMicroVMExecServer
}

func (f *e2eFakeMicroVMExec) ExecCommand(stream microvmexecv1alpha1.MicroVMExec_ExecCommandServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return stream.Send(&microvmexecv1alpha1.ExecCommandResponse{
		Payload: &microvmexecv1alpha1.ExecCommandResponse_ExitCode{ExitCode: 0},
	})
}

// startE2EFakeFlintlock starts the fake flintlock (MicroVM + MicroVMExec) on
// a real loopback listener and dials it as a single-host
// flintlockclient.Pool named "host-a".
func startE2EFakeFlintlock(t *testing.T) *flintlockclient.Pool {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	microvmv1alpha1.RegisterMicroVMServer(srv, &e2eFakeMicroVM{})
	microvmexecv1alpha1.RegisterMicroVMExecServer(srv, &e2eFakeMicroVMExec{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	pool, err := flintlockclient.New(&config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: lis.Addr().String(), TLS: config.TLSConfig{Insecure: true}},
	}})
	if err != nil {
		t.Fatalf("flintlockclient.New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	return pool
}

// e2ePoolmgrd bundles the client stubs a black-box e2e test needs.
type e2ePoolmgrd struct {
	PoolAdmin poolmgrv1alpha1.PoolAdminClient
	Lease     poolmgrv1alpha1.LeaseClient
	Events    poolmgrv1alpha1.EventsClient
}

// startE2EPoolmgrd builds and serves a real poolmgrd - the same
// construction main() uses (store, flint, poolmanager.Manager,
// buildGRPCServer) minus the CLI/config-file/signal-handling glue - on a
// loopback port, and returns gRPC client stubs dialed against it.
func startE2EPoolmgrd(t *testing.T, flint *flintlockclient.Pool) e2ePoolmgrd {
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

	srv, err := buildGRPCServer(config.APIServerConfig{Addr: ":0", TLS: config.ServerTLSConfig{Insecure: true}}, st, flint, reg, poolMgr)
	if err != nil {
		t.Fatalf("buildGRPCServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = serveGRPC(runCtx, srv, lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return e2ePoolmgrd{
		PoolAdmin: poolmgrv1alpha1.NewPoolAdminClient(conn),
		Lease:     poolmgrv1alpha1.NewLeaseClient(conn),
		Events:    poolmgrv1alpha1.NewEventsClient(conn),
	}
}

// e2eMinSizeThresholdPool returns a minimal, valid PoolSpec on host-a that
// keeps exactly minSize VMs warm via MIN_SIZE_THRESHOLD.
func e2eMinSizeThresholdPool(name string, size, minSize int32) *poolmgrv1alpha1.PoolSpec {
	return &poolmgrv1alpha1.PoolSpec{
		Name:            name,
		Namespace:       "e2e",
		Size:            size,
		FlintlockHosts:  []string{"host-a"},
		MicrovmTemplate: &flintlocktypes.MicroVMSpec{Vcpu: 1, MemoryInMb: 1024},
		ReplenishmentStrategy: &poolmgrv1alpha1.ReplenishmentStrategy{
			Type:    poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD,
			MinSize: proto.Int32(minSize),
		},
		HookFailurePolicy:        poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE,
		HeartbeatInterval:        durationpb.New(30 * time.Second),
		HeartbeatExpiryThreshold: durationpb.New(90 * time.Second),
	}
}

// waitForAvailable polls GetPool until its available count equals want, or
// ctx expires.
func waitForAvailable(ctx context.Context, t *testing.T, admin poolmgrv1alpha1.PoolAdminClient, ref *poolmgrv1alpha1.PoolRef, want int32) *poolmgrv1alpha1.Pool {
	t.Helper()

	ticker := time.NewTicker(e2ePollInterval)
	defer ticker.Stop()

	for {
		pool, err := admin.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: ref})
		if err != nil {
			t.Fatalf("GetPool: %v", err)
		}
		if pool.GetStatus().GetAvailableCount() == want {
			return pool
		}

		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for pool %s/%s to reach available=%d (last status: %+v)", ref.GetNamespace(), ref.GetName(), want, pool.GetStatus())
		case <-ticker.C:
		}
	}
}

// e2eEventRecorder collects every Event a Subscribe stream delivers, safe
// for concurrent reads while the stream is still being drained.
type e2eEventRecorder struct {
	mu   sync.Mutex
	seen []poolmgrv1alpha1.EventType
}

func (r *e2eEventRecorder) add(t poolmgrv1alpha1.EventType) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, t)
}

func (r *e2eEventRecorder) has(t poolmgrv1alpha1.EventType) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.seen {
		if s == t {
			return true
		}
	}
	return false
}

// waitForEvent polls recorder until it has seen t, or ctx expires.
func waitForEvent(ctx context.Context, t *testing.T, recorder *e2eEventRecorder, want poolmgrv1alpha1.EventType) {
	t.Helper()

	ticker := time.NewTicker(e2ePollInterval)
	defer ticker.Stop()

	for {
		if recorder.has(want) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for event %s", want)
		case <-ticker.C:
		}
	}
}

// subscribeEvents opens an Events.Subscribe stream for ref and drains it
// into an e2eEventRecorder in the background until ctx is done. Subscribe
// replays the outbox's existing events on connect, so it's safe to call this
// either before or after the activity being observed.
func subscribeEvents(ctx context.Context, t *testing.T, events poolmgrv1alpha1.EventsClient, ref *poolmgrv1alpha1.PoolRef) *e2eEventRecorder {
	t.Helper()

	stream, err := events.Subscribe(ctx, &poolmgrv1alpha1.SubscribeRequest{Pool: ref})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	recorder := &e2eEventRecorder{}
	go func() {
		for {
			ev, err := stream.Recv()
			if err != nil {
				return
			}
			recorder.add(ev.GetType())
		}
	}()
	return recorder
}

// TestE2E_PoolLifecycle drives a pool through its full golden path purely
// via poolmgrd's public gRPC API: CreatePool, provisioning up to
// AVAILABLE, ClaimVM, Heartbeat, ReleaseVM, replenishment, and the
// Events.Subscribe stream those steps should produce. This is the flow
// docs/runbooks/e2e-manual-verification.md's steps 5-8 require a real
// Firecracker host to exercise manually.
func TestE2E_PoolLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), e2eTestTimeout)
	defer cancel()

	flint := startE2EFakeFlintlock(t)
	pm := startE2EPoolmgrd(t, flint)

	spec := e2eMinSizeThresholdPool("e2e-pool", 1, 1)
	ref := &poolmgrv1alpha1.PoolRef{Name: spec.GetName(), Namespace: spec.GetNamespace()}

	if _, err := pm.PoolAdmin.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	recorder := subscribeEvents(ctx, t, pm.Events, ref)

	pool := waitForAvailable(ctx, t, pm.PoolAdmin, ref, 1)
	if got := pool.GetStatus().GetLeasedCount(); got != 0 {
		t.Fatalf("expected leased=0 once provisioned, got %d", got)
	}
	waitForEvent(ctx, t, recorder, poolmgrv1alpha1.EventType_VM_PROVISIONED)
	waitForEvent(ctx, t, recorder, poolmgrv1alpha1.EventType_VM_AVAILABLE)

	claim, err := pm.Lease.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: ref})
	if err != nil {
		t.Fatalf("ClaimVM: %v", err)
	}
	if claim.GetLeaseId() == "" || claim.GetVmUid() == "" {
		t.Fatalf("ClaimVM returned an empty lease_id/vm_uid: %+v", claim)
	}
	if got := claim.GetHost().GetName(); got != "host-a" {
		t.Fatalf("ClaimVM host.name = %q, want host-a", got)
	}
	waitForEvent(ctx, t, recorder, poolmgrv1alpha1.EventType_VM_CLAIMED)

	if pool := mustGetPool(ctx, t, pm.PoolAdmin, ref); pool.GetStatus().GetAvailableCount() != 0 || pool.GetStatus().GetLeasedCount() != 1 {
		t.Fatalf("expected available=0/leased=1 right after claim, got %+v", pool.GetStatus())
	}

	if _, err := pm.Lease.Heartbeat(ctx, &poolmgrv1alpha1.HeartbeatRequest{LeaseId: claim.GetLeaseId()}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	if _, err := pm.Lease.ReleaseVM(ctx, &poolmgrv1alpha1.ReleaseVMRequest{LeaseId: claim.GetLeaseId()}); err != nil {
		t.Fatalf("ReleaseVM: %v", err)
	}
	waitForEvent(ctx, t, recorder, poolmgrv1alpha1.EventType_VM_DELETED_ON_RELEASE)

	// ReleaseVM's NotifyVMDeleted nudges the reconciler immediately, so the
	// pool replenishes back to available=1 without waiting on another tick.
	waitForAvailable(ctx, t, pm.PoolAdmin, ref, 1)

	// No VM-level drain/force-delete API exists yet (see DeletePool's own
	// doc comment) - deleting a pool that still owns a VM must fail rather
	// than orphan it.
	_, err = pm.PoolAdmin.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeletePool on a pool with a live VM: got err=%v, want FailedPrecondition", err)
	}
}

func mustGetPool(ctx context.Context, t *testing.T, admin poolmgrv1alpha1.PoolAdminClient, ref *poolmgrv1alpha1.PoolRef) *poolmgrv1alpha1.Pool {
	t.Helper()
	pool, err := admin.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: ref})
	if err != nil {
		t.Fatalf("GetPool: %v", err)
	}
	return pool
}

// TestE2E_ClaimFailsOnEmptyPool locks in the runbook's documented
// RESOURCE_EXHAUSTED behavior for an empty pool, and that such a pool (never
// having owned a VM) can be deleted immediately.
func TestE2E_ClaimFailsOnEmptyPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), e2eTestTimeout)
	defer cancel()

	flint := startE2EFakeFlintlock(t)
	pm := startE2EPoolmgrd(t, flint)

	spec := e2eMinSizeThresholdPool("e2e-empty-pool", 0, 1)
	// A zero-size pool never has an available VM to satisfy min_size=1 with,
	// so it never provisions - that's the point of this test, not a bug.
	ref := &poolmgrv1alpha1.PoolRef{Name: spec.GetName(), Namespace: spec.GetNamespace()}

	if _, err := pm.PoolAdmin.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	_, err := pm.Lease.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: ref})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("ClaimVM on an empty pool: got err=%v, want ResourceExhausted", err)
	}

	if _, err := pm.PoolAdmin.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: ref}); err != nil {
		t.Fatalf("DeletePool: %v", err)
	}

	_, err = pm.PoolAdmin.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: ref})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetPool after DeletePool: got err=%v, want NotFound", err)
	}
}
