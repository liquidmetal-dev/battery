package reconciler_test

import (
	"context"
	"strings"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/metrics"
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
	reg := metrics.NewRegistry()
	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, notifier, reg)
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

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if got := eventTypes(t, events); len(got) != 1 || got[0] != poolmgrv1alpha1.EventType_VM_DELETED_DUE_TO_EXPIRY {
		t.Fatalf("unexpected events: %v", got)
	}

	if len(notifier.deleted) != 1 || notifier.deleted[0] != "default/pool-a" {
		t.Fatalf("expected NotifyVMDeleted(pool-a) once, got %v", notifier.deleted)
	}

	body := scrapeMetrics(t, reg)
	if !strings.Contains(body, `poolmgr_vm_releases_total{pool_name="pool-a",pool_namespace="default",reason="expiry"} 1`) {
		t.Fatalf("expected expiry release metric, got:\n%s", body)
	}
	if !strings.Contains(body, `poolmgr_lease_duration_seconds_count{pool_name="pool-a",pool_namespace="default"} 1`) {
		t.Fatalf("expected lease duration observation, got:\n%s", body)
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

	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, nil, nil)

	sweeper.Tick(ctx, now)
	sweeper.Tick(ctx, now.Add(time.Second))
	sweeper.Tick(ctx, now.Add(2*time.Second))

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
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

	events, err = st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
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

	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, nil, nil)
	sweeper.Tick(ctx, now)

	if _, err := st.GetLease(ctx, "lease-1"); err == nil {
		t.Fatalf("expected stale lease row to be dropped")
	}
	if len(vm.deletedUIDs()) != 0 {
		t.Fatalf("expected no DeleteMicroVM calls for a VM that's already gone, got %v", vm.deletedUIDs())
	}
	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no events for a stale lease, got %v", eventTypes(t, events))
	}
}

// TestSweeper_HeartbeatDuringSweepIsNotLost is the deterministic
// interleaving test for the TOCTOU race between the sweeper observing an
// expired lease and a concurrent Heartbeat renewing it: the renewal is
// simulated by calling UpdateLeaseHeartbeat directly (the same store method
// the Heartbeat RPC handler uses) after the lease has become "expired as of
// now" but before Tick is invoked for that instant.
func TestSweeper_HeartbeatDuringSweepIsNotLost(t *testing.T) {
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

	// A heartbeat renews the lease before the sweeper's tick for `now` runs
	// (e.g. it landed between the sweeper's ListExpiredLeases snapshot and
	// its subsequent delete in a real concurrent run).
	renewedExpiry := now.Add(time.Hour)
	if err := st.UpdateLeaseHeartbeat(ctx, "lease-1", now, renewedExpiry); err != nil {
		t.Fatalf("UpdateLeaseHeartbeat: %v", err)
	}

	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, nil, nil)
	sweeper.Tick(ctx, now)

	gotVM, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("expected VM to survive the sweep, GetVM error = %v", err)
	}
	if gotVM.GetPhase() != poolmgrv1alpha1.VMPhase_LEASED {
		t.Fatalf("expected VM to remain LEASED, got %v", gotVM.GetPhase())
	}
	gotLease, err := st.GetLease(ctx, "lease-1")
	if err != nil {
		t.Fatalf("expected lease to survive the sweep, GetLease error = %v", err)
	}
	if !gotLease.GetExpiresAt().AsTime().Equal(renewedExpiry) {
		t.Fatalf("GetLease() expires_at = %v, want %v (renewal must survive)", gotLease.GetExpiresAt().AsTime(), renewedExpiry)
	}
	if len(vm.deletedUIDs()) != 0 {
		t.Fatalf("expected no DeleteMicroVM calls for a renewed lease, got %v", vm.deletedUIDs())
	}
}

// TestSweeper_RetriesPendingDeletionAcrossTicks covers the durable-retry
// fix: a flintlock DeleteMicroVM failure leaves the VM DELETING (no event,
// no notify) rather than being silently dropped, and a later tick finishes
// the deletion once flintlock succeeds.
func TestSweeper_RetriesPendingDeletionAcrossTicks(t *testing.T) {
	vm := &fakeMicroVM{failDeletesRemaining: 1}
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
	reg := metrics.NewRegistry()
	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, notifier, reg)

	// First tick: the lease is correctly claimed and deleted, but flintlock
	// fails once - the VM must be left DELETING, not silently forgotten.
	sweeper.Tick(ctx, now)

	gotVM, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("expected VM record to survive a failed flintlock delete, GetVM error = %v", err)
	}
	if gotVM.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Fatalf("expected VM to be left DELETING, got %v", gotVM.GetPhase())
	}
	if _, err := st.GetLease(ctx, "lease-1"); err == nil {
		t.Fatalf("expected the lease row to already be gone (claimed before the flintlock attempt)")
	}
	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no VM_DELETED_* event until deletion is confirmed, got %v", eventTypes(t, events))
	}
	if len(notifier.deleted) != 0 {
		t.Fatalf("expected no NotifyVMDeleted until deletion is confirmed, got %v", notifier.deleted)
	}
	// The lease already durably expired on this first tick (before the
	// flintlock delete attempt failed), so the release/duration metrics are
	// already recorded here - independent of retryPendingDeletions finishing
	// the actual VM deletion later.
	if body := scrapeMetrics(t, reg); !strings.Contains(body, `poolmgr_vm_releases_total{pool_name="pool-a",pool_namespace="default",reason="expiry"} 1`) {
		t.Fatalf("expected expiry release metric to already be recorded after tick 1, got:\n%s", body)
	}

	// Second tick: retryPendingDeletions finishes the job.
	sweeper.Tick(ctx, now.Add(time.Second))

	if _, err := st.GetVM(ctx, "vm-1"); err == nil {
		t.Fatalf("expected VM record to be deleted after the retry succeeds")
	}
	events, err = st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 1 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_DELETED_DUE_TO_EXPIRY {
		t.Fatalf("expected 1 VM_DELETED_DUE_TO_EXPIRY event after retry, got %v", eventTypes(t, events))
	}
	if len(notifier.deleted) != 1 || notifier.deleted[0] != "default/pool-a" {
		t.Fatalf("expected NotifyVMDeleted(pool-a) once after retry, got %v", notifier.deleted)
	}

	// retryPendingDeletions' call to FinishVMDeletion must not double-count
	// the release/duration metrics already recorded on tick 1.
	if body := scrapeMetrics(t, reg); !strings.Contains(body, `poolmgr_vm_releases_total{pool_name="pool-a",pool_namespace="default",reason="expiry"} 1`) {
		t.Fatalf("expected expiry release metric to still be exactly 1 after retry, got:\n%s", body)
	}
}
