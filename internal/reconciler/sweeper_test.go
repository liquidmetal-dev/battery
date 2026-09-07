package reconciler_test

import (
	"context"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/reconciler"
)

// spyNotifier records NotifyVMDeleted calls.
type spyNotifier struct {
	deleted []string // "poolNamespace/poolName"
}

func (s *spyNotifier) NotifyVMDeleted(poolName, poolNamespace string) {
	s.deleted = append(s.deleted, poolNamespace+"/"+poolName)
}

func sampleLease(leaseID, vmUID, poolName string, expiresAt time.Time) *poolmgrv1alpha1.LeaseRecord {
	now := timestamppb.Now()
	return &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         leaseID,
		VmUid:           vmUID,
		PoolName:        poolName,
		PoolNamespace:   "default",
		ClaimedAt:       now,
		LastHeartbeatAt: now,
		ExpiresAt:       timestamppb.New(expiresAt),
	}
}

func eventTypes(t *testing.T, events []*poolmgrv1alpha1.Event) []poolmgrv1alpha1.EventType {
	t.Helper()
	out := make([]poolmgrv1alpha1.EventType, len(events))
	for i, e := range events {
		out[i] = e.GetType()
	}
	return out
}

func TestSweeper_ExpiresLeaseWithNoHeartbeat(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 1, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	leasedVM := sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_LEASED)
	if err := st.CreateVM(ctx, leasedVM); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	now := time.Now()
	lease := sampleLease("lease-1", "vm-1", "pool-a", now.Add(-time.Second))
	if err := st.CreateLease(ctx, lease); err != nil {
		t.Fatalf("CreateLease: %v", err)
	}

	notifier := &spyNotifier{}
	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, notifier)
	sweeper.Tick(ctx, now)

	if _, err := st.GetVM(ctx, "vm-1"); err == nil {
		t.Fatalf("expected VM to be deleted")
	}
	if _, err := st.GetLease(ctx, "lease-1"); err == nil {
		t.Fatalf("expected lease to be deleted")
	}
	if got := vm.deletedUIDs(); len(got) != 1 || got[0] != "vm-1" {
		t.Fatalf("expected DeleteMicroVM(vm-1) to be called once, got %v", got)
	}

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if got := eventTypes(t, events); len(got) != 1 || got[0] != poolmgrv1alpha1.EventType_VM_DELETED_DUE_TO_EXPIRY {
		t.Fatalf("unexpected events: %v", got)
	}

	if len(notifier.deleted) != 1 || notifier.deleted[0] != "default/pool-a" {
		t.Fatalf("expected NotifyVMDeleted(pool-a) once, got %v", notifier.deleted)
	}
}

func TestSweeper_WarnsOnceUntilHeartbeatResets(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 1, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	leasedVM := sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_LEASED)
	if err := st.CreateVM(ctx, leasedVM); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	now := time.Now()
	firstExpiry := now.Add(10 * time.Second) // within a 30s warning window, not yet expired
	lease := sampleLease("lease-1", "vm-1", "pool-a", firstExpiry)
	if err := st.CreateLease(ctx, lease); err != nil {
		t.Fatalf("CreateLease: %v", err)
	}

	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, nil)

	sweeper.Tick(ctx, now)
	sweeper.Tick(ctx, now.Add(time.Second))
	sweeper.Tick(ctx, now.Add(2*time.Second))

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if got := eventTypes(t, events); len(got) != 1 || got[0] != poolmgrv1alpha1.EventType_VM_EXPIRING_SOON {
		t.Fatalf("expected exactly one VM_EXPIRING_SOON across repeated ticks, got %v", got)
	}
	if len(vm.deletedUIDs()) != 0 {
		t.Fatalf("expected no DeleteMicroVM calls, got %v", vm.deletedUIDs())
	}

	// A heartbeat moves expires_at forward into a new near-future value:
	// the dedup key changes, so a second, distinct VM_EXPIRING_SOON fires.
	secondExpiry := now.Add(20 * time.Second)
	if err := st.UpdateLeaseHeartbeat(ctx, "lease-1", now.Add(3*time.Second), secondExpiry); err != nil {
		t.Fatalf("UpdateLeaseHeartbeat: %v", err)
	}
	sweeper.Tick(ctx, now.Add(3*time.Second))

	events, err = st.ListEventsSince(ctx, "pool-a", "default", 0)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	got := eventTypes(t, events)
	if len(got) != 2 || got[0] != poolmgrv1alpha1.EventType_VM_EXPIRING_SOON || got[1] != poolmgrv1alpha1.EventType_VM_EXPIRING_SOON {
		t.Fatalf("expected a second distinct VM_EXPIRING_SOON after heartbeat, got %v", got)
	}
}

func TestSweeper_SkipsVMAlreadyReleased(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 1, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	// VM row already gone (as if a concurrent ReleaseVM completed), but the
	// lease row is still present and expired.
	now := time.Now()
	lease := sampleLease("lease-1", "vm-1", "pool-a", now.Add(-time.Second))
	if err := st.CreateLease(ctx, lease); err != nil {
		t.Fatalf("CreateLease: %v", err)
	}

	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, nil)
	sweeper.Tick(ctx, now)

	if _, err := st.GetLease(ctx, "lease-1"); err == nil {
		t.Fatalf("expected stale lease row to be dropped")
	}
	if len(vm.deletedUIDs()) != 0 {
		t.Fatalf("expected no DeleteMicroVM calls for a VM that's already gone, got %v", vm.deletedUIDs())
	}
	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no events for a stale lease, got %v", eventTypes(t, events))
	}
}
