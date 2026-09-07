package api_test

import (
	"context"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liquidmetal-dev/battery/internal/api"
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
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier)

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

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 1 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_CLAIMED {
		t.Fatalf("expected 1 VM_CLAIMED event, got %+v", events)
	}

	if len(notifier.claimed) != 1 || notifier.claimed[0] != "default/pool-a" {
		t.Fatalf("expected NotifyVMClaimed(pool-a) once, got %v", notifier.claimed)
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

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil)
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

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil)
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
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier)
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

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 1 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_HOOK_FAILED {
		t.Fatalf("expected 1 VM_HOOK_FAILED event, got %+v", events)
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
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier)
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

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil)
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

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil)
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
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier)
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

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0)
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
	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier)
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

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil)
	_, err := s.ReleaseVM(ctx, &poolmgrv1alpha1.ReleaseVMRequest{LeaseId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}
