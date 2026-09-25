package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func TestClaimVM_Success(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	notifier := &spyNotifier{}
	reg := metrics.NewRegistry()
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier, reg)

	resp, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("ClaimVM: %v", err)
	}
	if resp.GetLeaseId() == "" {
		t.Fatalf("expected non-empty lease id")
	}
	if resp.GetVmUid() != "vm-1" {
		t.Fatalf("expected vm_uid vm-1, got %s", resp.GetVmUid())
	}
	if len(resp.GetNetworkInterfaces()) != 1 {
		t.Fatalf("expected 1 network interface, got %v", resp.GetNetworkInterfaces())
	}
	wantAddr, err := flint.Address("host-a")
	if err != nil {
		t.Fatalf("Address: %v", err)
	}
	if resp.GetHost().GetName() != "host-a" || resp.GetHost().GetAddress() != wantAddr {
		t.Fatalf("expected host {host-a %s}, got %v", wantAddr, resp.GetHost())
	}

	gotVM, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM: %v", err)
	}
	if gotVM.GetPhase() != poolmgrv1alpha1.VMPhase_LEASED {
		t.Fatalf("expected phase LEASED, got %v", gotVM.GetPhase())
	}

	lease, err := st.GetLease(ctx, resp.GetLeaseId())
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if lease.GetVmUid() != "vm-1" {
		t.Fatalf("expected lease vm_uid vm-1, got %s", lease.GetVmUid())
	}

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 1 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_CLAIMED {
		t.Fatalf("expected 1 VM_CLAIMED event, got %+v", events)
	}

	if len(notifier.claimed) != 1 || notifier.claimed[0] != "default/pool-a" {
		t.Fatalf("expected NotifyVMClaimed(pool-a) once, got %v", notifier.claimed)
	}

	if body := scrapeMetrics(t, reg); !strings.Contains(body, `poolmgr_vm_claims_total{pool_name="pool-a",pool_namespace="default",replayed="false"} 1`) {
		t.Fatalf("expected 1 vm claim recorded, got:\n%s", body)
	}
}

func TestClaimVM_NoAvailableVM(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	_, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
}

func TestClaimVM_UnknownPool(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	_, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "does-not-exist", Namespace: "default"}})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestClaimVM_PreLeaseHookFailure_Quarantine(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{
		respond: func(*microvmexecv1alpha1.ExecStart) (int32, string) { return 1, "" },
	}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, []string{"do-thing"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	notifier := &spyNotifier{}
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier, nil)
	_, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}

	gotVM, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM: %v", err)
	}
	if gotVM.GetPhase() != poolmgrv1alpha1.VMPhase_QUARANTINED {
		t.Fatalf("expected phase QUARANTINED, got %v", gotVM.GetPhase())
	}
	if gotVM.GetLeaseId() != "" {
		t.Fatalf("expected lease_id to be cleared on quarantine, got %q", gotVM.GetLeaseId())
	}
	if len(vm.deletedUIDs()) != 0 {
		t.Fatalf("expected no DeleteMicroVM calls under QUARANTINE, got %v", vm.deletedUIDs())
	}
	if len(notifier.deleted) != 0 {
		t.Fatalf("expected no NotifyVMDeleted under QUARANTINE (VM wasn't deleted), got %v", notifier.deleted)
	}

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 1 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_HOOK_FAILED {
		t.Fatalf("expected 1 VM_HOOK_FAILED event, got %+v", events)
	}
}

// cancelAfterClaimStore wraps a store.Store and cancels a context right
// after ClaimAvailableVM succeeds, simulating a caller disconnecting (or a
// deadline expiring) at exactly that instant - the earliest point cleanup
// must survive, since the VM is already committed as claimed.
type cancelAfterClaimStore struct {
	store.Store
	cancel context.CancelFunc
}

func (s cancelAfterClaimStore) ClaimAvailableVM(ctx context.Context, poolName, poolNamespace string) (*poolmgrv1alpha1.VMRecord, error) {
	vm, err := s.Store.ClaimAvailableVM(ctx, poolName, poolNamespace)
	if err == nil {
		s.cancel()
	}
	return vm, err
}

// TestClaimVM_CleanupSurvivesCancellationAfterClaim reproduces cancelling
// the RPC's context immediately after ClaimAvailableVM commits (before any
// pre-lease hook runs): cleanup must still quarantine/delete the VM rather
// than leaving it LEASED with no lease and nothing to retry it.
func TestClaimVM_CleanupSurvivesCancellationAfterClaim(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	setupCtx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if err := st.CreatePool(setupCtx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(setupCtx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelingStore := cancelAfterClaimStore{Store: st, cancel: cancel}

	s := api.NewLeaseServer(cancelingStore, flint, api.HookExecConfig{}, nil, nil)
	if _, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}}); err == nil {
		t.Fatalf("expected ClaimVM to fail once the context is cancelled mid-flow")
	}

	gotVM, err := st.GetVM(setupCtx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM: %v", err)
	}
	if gotVM.GetPhase() != poolmgrv1alpha1.VMPhase_QUARANTINED {
		t.Fatalf("expected cleanup to still quarantine the VM despite a cancelled context, got phase %v", gotVM.GetPhase())
	}
	if gotVM.GetLeaseId() != "" {
		t.Fatalf("expected lease_id to be cleared, got %q", gotVM.GetLeaseId())
	}
}

// TestClaimVM_CleanupSurvivesCancellationDuringHook reproduces the RPC's
// context being cancelled while a pre-lease hook is failing (e.g. the
// caller disconnected right as the hook errored): cleanup must still run
// to completion on a context independent of the cancelled one.
func TestClaimVM_CleanupSurvivesCancellationDuringHook(t *testing.T) {
	vm := &fakeMicroVM{}
	var cancel context.CancelFunc
	exec := &fakeMicroVMExec{
		respond: func(*microvmexecv1alpha1.ExecStart) (int32, string) {
			cancel() // simulate the client disconnecting right as the hook fails
			return 1, ""
		},
	}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	setupCtx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE, []string{"do-thing"})
	if err := st.CreatePool(setupCtx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(setupCtx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	notifier := &spyNotifier{}
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier, nil)
	if _, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}}); err == nil {
		t.Fatalf("expected ClaimVM to fail")
	}

	if _, err := st.GetVM(setupCtx, "vm-1"); err != store.ErrNotFound {
		t.Fatalf("expected cleanup to still delete the VM despite cancellation mid-hook, GetVM error = %v", err)
	}
	if len(notifier.deleted) != 1 {
		t.Fatalf("expected NotifyVMDeleted despite cancellation mid-hook, got %v", notifier.deleted)
	}
}

func TestClaimVM_PreLeaseHookFailure_DeleteAndReplace(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{
		respond: func(*microvmexecv1alpha1.ExecStart) (int32, string) { return 1, "" },
	}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE, []string{"do-thing"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	notifier := &spyNotifier{}
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier, nil)
	_, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}

	if _, err := st.GetVM(ctx, "vm-1"); err != store.ErrNotFound {
		t.Fatalf("expected VM record to be deleted, GetVM error = %v", err)
	}
	if got := vm.deletedUIDs(); len(got) != 1 || got[0] != "vm-1" {
		t.Fatalf("expected DeleteMicroVM(vm-1) once, got %v", got)
	}
	if len(notifier.deleted) != 1 || notifier.deleted[0] != "default/pool-a" {
		t.Fatalf("expected NotifyVMDeleted(pool-a) once after a DELETE_AND_REPLACE hook failure, got %v", notifier.deleted)
	}
}

// claimRequest returns a ClaimVMRequest for poolName in "default" with
// requestID.
func claimRequest(poolName, requestID string) *poolmgrv1alpha1.ClaimVMRequest {
	return &poolmgrv1alpha1.ClaimVMRequest{
		Pool:      &poolmgrv1alpha1.PoolRef{Name: poolName, Namespace: "default"},
		RequestId: requestID,
	}
}

// setupPoolWithVMs creates pool (in "default") and an AVAILABLE VM for
// each of uids.
func setupPoolWithVMs(t *testing.T, st store.Store, pool *poolmgrv1alpha1.PoolSpec, uids ...string) {
	t.Helper()
	ctx := context.Background()
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	for _, uid := range uids {
		if err := st.CreateVM(ctx, sampleAvailableVM(uid, pool.GetName())); err != nil {
			t.Fatalf("CreateVM(%s): %v", uid, err)
		}
	}
}

// assertVMPhase fails the test unless uid's VM is in phase want.
func assertVMPhase(t *testing.T, st store.Store, uid string, want poolmgrv1alpha1.VMPhase) {
	t.Helper()
	got, err := st.GetVM(context.Background(), uid)
	if err != nil {
		t.Fatalf("GetVM(%s): %v", uid, err)
	}
	if got.GetPhase() != want {
		t.Fatalf("expected %s phase %v, got %v", uid, want, got.GetPhase())
	}
}

func TestClaimVM_EmptyRequestIDClaimsEachTime(t *testing.T) {
	flint := startFakeFlintlock(t, &fakeMicroVM{}, &fakeMicroVMExec{})
	st := openTestStore(t)
	ctx := context.Background()
	setupPoolWithVMs(t, st, samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil), "vm-1", "vm-2")

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	first, err := s.ClaimVM(ctx, claimRequest("pool-a", ""))
	if err != nil {
		t.Fatalf("first ClaimVM: %v", err)
	}
	second, err := s.ClaimVM(ctx, claimRequest("pool-a", ""))
	if err != nil {
		t.Fatalf("second ClaimVM: %v", err)
	}
	if first.GetLeaseId() == second.GetLeaseId() || first.GetVmUid() == second.GetVmUid() {
		t.Fatalf("expected two distinct claims, got %+v and %+v", first, second)
	}

	lease, err := st.GetLease(ctx, first.GetLeaseId())
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if lease.GetRequestId() != "" {
		t.Fatalf("expected empty request_id on lease, got %q", lease.GetRequestId())
	}
}

func TestClaimVM_RequestIDTooLong(t *testing.T) {
	flint := startFakeFlintlock(t, &fakeMicroVM{}, &fakeMicroVMExec{})
	st := openTestStore(t)
	ctx := context.Background()
	setupPoolWithVMs(t, st, samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil), "vm-1")

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	_, err := s.ClaimVM(ctx, claimRequest("pool-a", strings.Repeat("x", 256)))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
	assertVMPhase(t, st, "vm-1", poolmgrv1alpha1.VMPhase_AVAILABLE)

	if _, err := s.ClaimVM(ctx, claimRequest("pool-a", strings.Repeat("x", 255))); err != nil {
		t.Fatalf("ClaimVM with a 255-byte request_id: %v", err)
	}
}

func TestClaimVM_ReplayReturnsSameLease(t *testing.T) {
	flint := startFakeFlintlock(t, &fakeMicroVM{}, &fakeMicroVMExec{})
	st := openTestStore(t)
	ctx := context.Background()
	setupPoolWithVMs(t, st, samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, []string{"do-thing"}), "vm-1", "vm-2")

	notifier := &spyNotifier{}
	reg := metrics.NewRegistry()
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier, reg)

	first, err := s.ClaimVM(ctx, claimRequest("pool-a", "req-1"))
	if err != nil {
		t.Fatalf("first ClaimVM: %v", err)
	}
	before, err := st.GetLease(ctx, first.GetLeaseId())
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if before.GetRequestId() != "req-1" {
		t.Fatalf("expected lease request_id req-1, got %q", before.GetRequestId())
	}

	time.Sleep(2 * time.Millisecond) // a heartbeat now would move expires_at
	second, err := s.ClaimVM(ctx, claimRequest("pool-a", "req-1"))
	if err != nil {
		t.Fatalf("replayed ClaimVM: %v", err)
	}
	if !proto.Equal(first, second) {
		t.Fatalf("expected replay to return the same response\nfirst:  %+v\nsecond: %+v", first, second)
	}

	after, err := st.GetLease(ctx, first.GetLeaseId())
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if !after.GetExpiresAt().AsTime().Equal(before.GetExpiresAt().AsTime()) {
		t.Fatalf("expected replay to leave expires_at alone, was %v, now %v", before.GetExpiresAt().AsTime(), after.GetExpiresAt().AsTime())
	}

	other := "vm-1"
	if first.GetVmUid() == "vm-1" {
		other = "vm-2"
	}
	assertVMPhase(t, st, other, poolmgrv1alpha1.VMPhase_AVAILABLE)

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 1 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_CLAIMED {
		t.Fatalf("expected 1 VM_CLAIMED event, got %+v", events)
	}
	if len(notifier.claimed) != 1 {
		t.Fatalf("expected NotifyVMClaimed once, got %v", notifier.claimed)
	}

	body := scrapeMetrics(t, reg)
	for _, want := range []string{
		`poolmgr_vm_claims_total{pool_name="pool-a",pool_namespace="default",replayed="false"} 1`,
		`poolmgr_vm_claims_total{pool_name="pool-a",pool_namespace="default",replayed="true"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected %s, got:\n%s", want, body)
		}
	}
}

func TestClaimVM_ReplayDifferentPool(t *testing.T) {
	flint := startFakeFlintlock(t, &fakeMicroVM{}, &fakeMicroVMExec{})
	st := openTestStore(t)
	ctx := context.Background()
	setupPoolWithVMs(t, st, samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil), "vm-a1")
	setupPoolWithVMs(t, st, samplePool("pool-b", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil), "vm-b1")

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	if _, err := s.ClaimVM(ctx, claimRequest("pool-a", "req-1")); err != nil {
		t.Fatalf("ClaimVM(pool-a): %v", err)
	}
	_, err := s.ClaimVM(ctx, claimRequest("pool-b", "req-1"))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
	assertVMPhase(t, st, "vm-b1", poolmgrv1alpha1.VMPhase_AVAILABLE)
}

func TestClaimVM_RequestIDReusableAfterRelease(t *testing.T) {
	flint := startFakeFlintlock(t, &fakeMicroVM{}, &fakeMicroVMExec{})
	st := openTestStore(t)
	ctx := context.Background()
	setupPoolWithVMs(t, st, samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil), "vm-1", "vm-2")

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	first, err := s.ClaimVM(ctx, claimRequest("pool-a", "req-1"))
	if err != nil {
		t.Fatalf("first ClaimVM: %v", err)
	}
	if _, err := s.ReleaseVM(ctx, &poolmgrv1alpha1.ReleaseVMRequest{LeaseId: first.GetLeaseId()}); err != nil {
		t.Fatalf("ReleaseVM: %v", err)
	}

	second, err := s.ClaimVM(ctx, claimRequest("pool-a", "req-1"))
	if err != nil {
		t.Fatalf("second ClaimVM: %v", err)
	}
	if second.GetLeaseId() == first.GetLeaseId() || second.GetVmUid() == first.GetVmUid() {
		t.Fatalf("expected a fresh claim after release, got %+v (first was %+v)", second, first)
	}
}

func TestClaimVM_ReplayVMGone(t *testing.T) {
	flint := startFakeFlintlock(t, &fakeMicroVM{}, &fakeMicroVMExec{})
	st := openTestStore(t)
	ctx := context.Background()
	setupPoolWithVMs(t, st, samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil), "vm-1")

	// A lease whose VM row is already gone: ReleaseVM deletes the VM
	// before the lease.
	now := timestamppb.Now()
	if err := st.CreateLease(ctx, &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         "lease-1",
		VmUid:           "vm-gone",
		PoolName:        "pool-a",
		PoolNamespace:   "default",
		ClaimedAt:       now,
		LastHeartbeatAt: now,
		ExpiresAt:       now,
		RequestId:       "req-1",
	}); err != nil {
		t.Fatalf("CreateLease: %v", err)
	}

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	_, err := s.ClaimVM(ctx, claimRequest("pool-a", "req-1"))
	if status.Code(err) != codes.Aborted {
		t.Fatalf("expected Aborted, got %v", err)
	}
	assertVMPhase(t, st, "vm-1", poolmgrv1alpha1.VMPhase_AVAILABLE)
}

// racingCreateLeaseStore wraps a store.Store and, on the first CreateLease
// with a request_id, first claims another VM and commits a lease for it
// with the same request_id. This reproduces two ClaimVMs with one
// request_id that both passed the lookup, with the other one winning.
type racingCreateLeaseStore struct {
	store.Store

	mu          sync.Mutex
	winnerLease *poolmgrv1alpha1.LeaseRecord
}

func (s *racingCreateLeaseStore) CreateLease(ctx context.Context, l *poolmgrv1alpha1.LeaseRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.GetRequestId() == "" || s.winnerLease != nil {
		return s.Store.CreateLease(ctx, l)
	}

	vm, err := s.ClaimAvailableVM(ctx, l.GetPoolName(), l.GetPoolNamespace())
	if err != nil {
		return fmt.Errorf("racer: claim: %w", err)
	}
	winner := proto.Clone(l).(*poolmgrv1alpha1.LeaseRecord)
	winner.LeaseId = "winner-lease"
	winner.VmUid = vm.GetUid()
	vm.LeaseId = &winner.LeaseId
	if err := s.UpdateVM(ctx, vm); err != nil {
		return fmt.Errorf("racer: update vm: %w", err)
	}
	if err := s.Store.CreateLease(ctx, winner); err != nil {
		return fmt.Errorf("racer: create lease: %w", err)
	}
	s.winnerLease = winner
	return s.Store.CreateLease(ctx, l)
}

func TestClaimVM_ConcurrentDuplicateRequestID(t *testing.T) {
	flint := startFakeFlintlock(t, &fakeMicroVM{}, &fakeMicroVMExec{})
	st := openTestStore(t)
	ctx := context.Background()
	setupPoolWithVMs(t, st, samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, []string{"do-thing"}), "vm-1", "vm-2")

	racer := &racingCreateLeaseStore{Store: st}
	notifier := &spyNotifier{}
	reg := metrics.NewRegistry()
	s := api.NewLeaseServer(racer, flint, api.HookExecConfig{}, notifier, reg)

	resp, err := s.ClaimVM(ctx, claimRequest("pool-a", "req-1"))
	if err != nil {
		t.Fatalf("ClaimVM: %v", err)
	}
	winner := racer.winnerLease
	if winner == nil {
		t.Fatalf("expected the racer to have created a competing lease")
	}
	if resp.GetLeaseId() != winner.GetLeaseId() || resp.GetVmUid() != winner.GetVmUid() {
		t.Fatalf("expected the winner's lease %s on %s, got lease %s on %s",
			winner.GetLeaseId(), winner.GetVmUid(), resp.GetLeaseId(), resp.GetVmUid())
	}
	if len(resp.GetNetworkInterfaces()) != 1 {
		t.Fatalf("expected 1 network interface, got %v", resp.GetNetworkInterfaces())
	}

	loser := "vm-1"
	if winner.GetVmUid() == "vm-1" {
		loser = "vm-2"
	}
	gotLoser, err := st.GetVM(ctx, loser)
	if err != nil {
		t.Fatalf("GetVM(%s): %v", loser, err)
	}
	if gotLoser.GetPhase() != poolmgrv1alpha1.VMPhase_AVAILABLE || gotLoser.GetLeaseId() != "" {
		t.Fatalf("expected loser %s back to AVAILABLE with no lease, got phase %v lease %q", loser, gotLoser.GetPhase(), gotLoser.GetLeaseId())
	}

	leases, err := st.ListLeases(ctx, nil)
	if err != nil {
		t.Fatalf("ListLeases: %v", err)
	}
	if len(leases) != 1 || leases[0].GetLeaseId() != winner.GetLeaseId() {
		t.Fatalf("expected only the winner's lease, got %+v", leases)
	}

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no events from the losing claim, got %+v", events)
	}
	if len(notifier.claimed) != 0 || len(notifier.deleted) != 0 {
		t.Fatalf("expected no notifications from the losing claim, got claimed=%v deleted=%v", notifier.claimed, notifier.deleted)
	}

	body := scrapeMetrics(t, reg)
	if !strings.Contains(body, `poolmgr_vm_claims_total{pool_name="pool-a",pool_namespace="default",replayed="true"} 1`) {
		t.Fatalf("expected 1 replayed claim recorded, got:\n%s", body)
	}
	if strings.Contains(body, `replayed="false"`) {
		t.Fatalf("expected no fresh claim recorded for the losing request, got:\n%s", body)
	}
}

func TestClaimVM_HookFailureThenRetryClaimsFresh(t *testing.T) {
	var calls atomic.Int32
	exec := &fakeMicroVMExec{
		respond: func(*microvmexecv1alpha1.ExecStart) (int32, string) {
			if calls.Add(1) == 1 {
				return 1, ""
			}
			return 0, ""
		},
	}
	flint := startFakeFlintlock(t, &fakeMicroVM{}, exec)
	st := openTestStore(t)
	ctx := context.Background()
	setupPoolWithVMs(t, st, samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, []string{"do-thing"}), "vm-1", "vm-2")

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	if _, err := s.ClaimVM(ctx, claimRequest("pool-a", "req-1")); status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal from the failing hook, got %v", err)
	}
	if _, err := st.GetLeaseByRequestID(ctx, "req-1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected no lease after a hook failure, GetLeaseByRequestID error = %v", err)
	}

	resp, err := s.ClaimVM(ctx, claimRequest("pool-a", "req-1"))
	if err != nil {
		t.Fatalf("retried ClaimVM: %v", err)
	}
	lease, err := st.GetLeaseByRequestID(ctx, "req-1")
	if err != nil {
		t.Fatalf("GetLeaseByRequestID: %v", err)
	}
	if lease.GetLeaseId() != resp.GetLeaseId() {
		t.Fatalf("expected the retry's lease %s to carry req-1, got %s", resp.GetLeaseId(), lease.GetLeaseId())
	}

	quarantined := "vm-1"
	if resp.GetVmUid() == "vm-1" {
		quarantined = "vm-2"
	}
	assertVMPhase(t, st, quarantined, poolmgrv1alpha1.VMPhase_QUARANTINED)
	assertVMPhase(t, st, resp.GetVmUid(), poolmgrv1alpha1.VMPhase_LEASED)
}

func TestHeartbeat_Success(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	claimResp, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("ClaimVM: %v", err)
	}

	before, err := st.GetLease(ctx, claimResp.GetLeaseId())
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}

	time.Sleep(2 * time.Millisecond) // ensure a measurable time delta
	hbResp, err := s.Heartbeat(ctx, &poolmgrv1alpha1.HeartbeatRequest{LeaseId: claimResp.GetLeaseId()})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !hbResp.GetExpiresAt().AsTime().After(before.GetExpiresAt().AsTime()) {
		t.Fatalf("expected extended expiry, got %v (was %v)", hbResp.GetExpiresAt().AsTime(), before.GetExpiresAt().AsTime())
	}

	after, err := st.GetLease(ctx, claimResp.GetLeaseId())
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if !after.GetExpiresAt().AsTime().Equal(hbResp.GetExpiresAt().AsTime()) {
		t.Fatalf("store expires_at %v does not match response %v", after.GetExpiresAt().AsTime(), hbResp.GetExpiresAt().AsTime())
	}
}

func TestHeartbeat_UnknownLease(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	_, err := s.Heartbeat(ctx, &poolmgrv1alpha1.HeartbeatRequest{LeaseId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestReleaseVM_Success(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	notifier := &spyNotifier{}
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier, nil)
	claimResp, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("ClaimVM: %v", err)
	}

	if _, err := s.ReleaseVM(ctx, &poolmgrv1alpha1.ReleaseVMRequest{LeaseId: claimResp.GetLeaseId()}); err != nil {
		t.Fatalf("ReleaseVM: %v", err)
	}

	if got := vm.deletedUIDs(); len(got) != 1 || got[0] != "vm-1" {
		t.Fatalf("expected DeleteMicroVM(vm-1) once, got %v", got)
	}
	if _, err := st.GetVM(ctx, "vm-1"); err != store.ErrNotFound {
		t.Fatalf("expected VM record to be deleted, GetVM error = %v", err)
	}
	if _, err := st.GetLease(ctx, claimResp.GetLeaseId()); err != store.ErrNotFound {
		t.Fatalf("expected lease to be deleted, GetLease error = %v", err)
	}

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 2 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_CLAIMED || events[1].GetType() != poolmgrv1alpha1.EventType_VM_DELETED_ON_RELEASE {
		t.Fatalf("unexpected events: %+v", events)
	}

	if len(notifier.deleted) != 1 || notifier.deleted[0] != "default/pool-a" {
		t.Fatalf("expected NotifyVMDeleted(pool-a) once, got %v", notifier.deleted)
	}
}

func TestReleaseVM_FlintlockDeleteFails_LeavesPendingForRetry(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	notifier := &spyNotifier{}
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier, nil)
	claimResp, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("ClaimVM: %v", err)
	}

	vm.mu.Lock()
	vm.deleteErr = status.Error(codes.Unavailable, "flintlock host unreachable")
	vm.mu.Unlock()

	_, err = s.ReleaseVM(ctx, &poolmgrv1alpha1.ReleaseVMRequest{LeaseId: claimResp.GetLeaseId()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}

	gotVM, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("expected VM record to survive a failed flintlock delete, GetVM error = %v", err)
	}
	if gotVM.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Fatalf("expected VM to be left DELETING, got %v", gotVM.GetPhase())
	}
	if _, err := st.GetLease(ctx, claimResp.GetLeaseId()); err != nil {
		t.Fatalf("expected lease row to remain until deletion is confirmed, GetLease error = %v", err)
	}
	if len(notifier.deleted) != 0 {
		t.Fatalf("expected no NotifyVMDeleted until deletion is confirmed, got %v", notifier.deleted)
	}
}

func TestReleaseVM_UnknownLease(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	_, err := s.ReleaseVM(ctx, &poolmgrv1alpha1.ReleaseVMRequest{LeaseId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestListLeases_Unfiltered(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	poolA := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	poolB := samplePool("pool-b", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if err := st.CreatePool(ctx, poolA); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreatePool(ctx, poolB); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-a1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-b1", "pool-b")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	claimA, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("ClaimVM(pool-a): %v", err)
	}
	claimB, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-b", Namespace: "default"}})
	if err != nil {
		t.Fatalf("ClaimVM(pool-b): %v", err)
	}

	resp, err := s.ListLeases(ctx, &poolmgrv1alpha1.ListLeasesRequest{})
	if err != nil {
		t.Fatalf("ListLeases: %v", err)
	}
	if len(resp.GetLeases()) != 2 {
		t.Fatalf("expected 2 leases, got %d: %+v", len(resp.GetLeases()), resp.GetLeases())
	}
	gotIDs := map[string]bool{}
	for _, l := range resp.GetLeases() {
		gotIDs[l.GetLeaseId()] = true
	}
	if !gotIDs[claimA.GetLeaseId()] || !gotIDs[claimB.GetLeaseId()] {
		t.Fatalf("expected leases from both pools, got %+v", resp.GetLeases())
	}
}

func TestListLeases_FilteredByPool(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	poolA := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	poolB := samplePool("pool-b", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if err := st.CreatePool(ctx, poolA); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreatePool(ctx, poolB); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-a1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-b1", "pool-b")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	claimA, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("ClaimVM(pool-a): %v", err)
	}
	if _, err := s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-b", Namespace: "default"}}); err != nil {
		t.Fatalf("ClaimVM(pool-b): %v", err)
	}

	resp, err := s.ListLeases(ctx, &poolmgrv1alpha1.ListLeasesRequest{PoolRef: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("ListLeases: %v", err)
	}
	if len(resp.GetLeases()) != 1 || resp.GetLeases()[0].GetLeaseId() != claimA.GetLeaseId() {
		t.Fatalf("expected only pool-a's lease, got %+v", resp.GetLeases())
	}
}

func TestListLeases_Empty(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	resp, err := s.ListLeases(ctx, &poolmgrv1alpha1.ListLeasesRequest{})
	if err != nil {
		t.Fatalf("ListLeases: %v", err)
	}
	if len(resp.GetLeases()) != 0 {
		t.Fatalf("expected empty result, got %+v", resp.GetLeases())
	}
}

// erroringListLeasesStore wraps a store.Store and forces ListLeases to fail,
// so ListLeases's store-error path can be exercised without a real store
// failure.
type erroringListLeasesStore struct {
	store.Store
}

func (erroringListLeasesStore) ListLeases(context.Context, *poolmgrv1alpha1.PoolRef) ([]*poolmgrv1alpha1.LeaseRecord, error) {
	return nil, errors.New("boom")
}

func TestListLeases_StoreError(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	s := api.NewLeaseServer(erroringListLeasesStore{Store: st}, flint, api.HookExecConfig{}, nil, nil)
	_, err := s.ListLeases(ctx, &poolmgrv1alpha1.ListLeasesRequest{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v", err)
	}
}
