package reconciler_test

import (
	"context"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func TestEnsureVMDeleted_Success(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	vmRecord := sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_LEASED)
	if err := st.CreateVM(ctx, vmRecord); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	if err := reconciler.EnsureVMDeleted(ctx, st, flint, vmRecord); err != nil {
		t.Fatalf("EnsureVMDeleted: %v", err)
	}

	if _, err := st.GetVM(ctx, "vm-1"); err != store.ErrNotFound {
		t.Fatalf("expected VM record to be deleted, GetVM error = %v", err)
	}
	if got := vm.deletedUIDs(); len(got) != 1 || got[0] != "vm-1" {
		t.Fatalf("expected DeleteMicroVM(vm-1) once, got %v", got)
	}
}

func TestEnsureVMDeleted_FlintlockUnavailable_LeavesDeletingForRetry(t *testing.T) {
	vm := &fakeMicroVM{failDeletesRemaining: 1}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	vmRecord := sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_LEASED)
	if err := st.CreateVM(ctx, vmRecord); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	if err := reconciler.EnsureVMDeleted(ctx, st, flint, vmRecord); err == nil {
		t.Fatalf("expected EnsureVMDeleted to return an error when flintlock is unavailable")
	}

	got, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM: %v", err)
	}
	if got.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Fatalf("expected VM to be left in DELETING for retry, got phase %v", got.GetPhase())
	}
	if len(vm.deletedUIDs()) != 0 {
		t.Fatalf("expected no successful DeleteMicroVM calls, got %v", vm.deletedUIDs())
	}

	// Retrying (as the sweeper's pending-deletion pass would) succeeds once
	// flintlock stops failing.
	if err := reconciler.EnsureVMDeleted(ctx, st, flint, got); err != nil {
		t.Fatalf("EnsureVMDeleted retry: %v", err)
	}
	if _, err := st.GetVM(ctx, "vm-1"); err != store.ErrNotFound {
		t.Fatalf("expected VM record to be deleted after retry, GetVM error = %v", err)
	}
}

func TestFinishVMDeletion_InfersExpiry(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 1, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	// No lease row exists for this vm (as if DeleteLeaseIfExpired already
	// removed it), so FinishVMDeletion should infer an expiry.
	vm := sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_DELETING)
	leaseID := "lease-gone"
	vm.LeaseId = &leaseID

	notifier := &spyNotifier{}
	reconciler.FinishVMDeletion(ctx, st, pool, vm, notifier)

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 1 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_DELETED_DUE_TO_EXPIRY {
		t.Fatalf("expected 1 VM_DELETED_DUE_TO_EXPIRY event, got %+v", events)
	}
	if len(notifier.deleted) != 1 || notifier.deleted[0] != "default/pool-a" {
		t.Fatalf("expected NotifyVMDeleted(pool-a) once, got %v", notifier.deleted)
	}
}

func TestFinishVMDeletion_InfersRelease(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 1, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	lease := sampleLease("lease-1", "vm-1", "pool-a", time.Now().Add(time.Hour))
	if err := st.CreateLease(ctx, lease); err != nil {
		t.Fatalf("CreateLease: %v", err)
	}
	vm := sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_DELETING)
	leaseID := "lease-1"
	vm.LeaseId = &leaseID

	notifier := &spyNotifier{}
	reconciler.FinishVMDeletion(ctx, st, pool, vm, notifier)

	if _, err := st.GetLease(ctx, "lease-1"); err != store.ErrNotFound {
		t.Fatalf("expected lease to be deleted, GetLease error = %v", err)
	}
	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 1 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_DELETED_ON_RELEASE {
		t.Fatalf("expected 1 VM_DELETED_ON_RELEASE event, got %+v", events)
	}
	if len(notifier.deleted) != 1 || notifier.deleted[0] != "default/pool-a" {
		t.Fatalf("expected NotifyVMDeleted(pool-a) once, got %v", notifier.deleted)
	}
}
