package reconciler_test

import (
	"context"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// staleVM returns a VMRecord for pool/host in phase, carrying templateHash
// and createdAt, suitable for seeding rollout tests where ordering and
// staleness both matter.
func staleVM(uid, poolName, host string, phase poolmgrv1alpha1.VMPhase, templateHash string, createdAt time.Time) *poolmgrv1alpha1.VMRecord {
	vm := sampleVM(uid, poolName, host, phase)
	vm.TemplateHash = templateHash
	vm.CreatedAt = timestamppb.New(createdAt)
	return vm
}

func TestRolloutController_DeletesOldestStaleAvailableVMsUpToBudget(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 5, []string{"host-a"})
	pool.TemplateHash = "new-hash"
	pool.RolloutPolicy = &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Count{Count: 2}}
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	now := time.Now()
	// Three stale AVAILABLE VMs, oldest to newest.
	oldest := staleVM("stale-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "old-hash", now.Add(-3*time.Hour))
	middle := staleVM("stale-2", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "old-hash", now.Add(-2*time.Hour))
	newest := staleVM("stale-3", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "old-hash", now.Add(-1*time.Hour))
	// A current-hash AVAILABLE VM: must never be touched.
	current := staleVM("current-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "new-hash", now.Add(-4*time.Hour))
	// A stale LEASED VM: must never be touched, even though it's stale.
	leased := staleVM("leased-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_LEASED, "old-hash", now.Add(-5*time.Hour))

	for _, v := range []*poolmgrv1alpha1.VMRecord{oldest, middle, newest, current, leased} {
		if err := st.CreateVM(ctx, v); err != nil {
			t.Fatalf("CreateVM(%s): %v", v.GetUid(), err)
		}
	}

	rc := reconciler.NewRolloutController(pool, st, flint, time.Second, nil, metrics.NewRegistry())
	rc.Tick(ctx, now)

	// Budget is 2: the two oldest stale AVAILABLE VMs are deleted.
	for _, uid := range []string{"stale-1", "stale-2"} {
		if _, err := st.GetVM(ctx, uid); err != store.ErrNotFound {
			t.Errorf("expected %s to be deleted, GetVM error = %v", uid, err)
		}
	}
	// The newest stale VM is left for a later tick (budget exhausted).
	if _, err := st.GetVM(ctx, "stale-3"); err != nil {
		t.Errorf("expected stale-3 to survive this tick, GetVM error = %v", err)
	}
	// Current-hash and LEASED VMs are never touched.
	if _, err := st.GetVM(ctx, "current-1"); err != nil {
		t.Errorf("expected current-1 (current hash) to survive, GetVM error = %v", err)
	}
	if _, err := st.GetVM(ctx, "leased-1"); err != nil {
		t.Errorf("expected leased-1 (LEASED) to survive, GetVM error = %v", err)
	}

	if got := vm.deletedUIDs(); len(got) != 2 {
		t.Fatalf("expected exactly 2 DeleteMicroVM calls, got %v", got)
	}

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if got := eventTypes(t, events); len(got) != 2 ||
		got[0] != poolmgrv1alpha1.EventType_VM_DELETED_FOR_ROLLOUT ||
		got[1] != poolmgrv1alpha1.EventType_VM_DELETED_FOR_ROLLOUT {
		t.Fatalf("expected 2 VM_DELETED_FOR_ROLLOUT events, got %v", got)
	}
}

func TestRolloutController_SubtractsInFlightDeletionsFromBudget(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 5, []string{"host-a"})
	pool.TemplateHash = "new-hash"
	pool.RolloutPolicy = &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Count{Count: 2}}
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	now := time.Now()
	// A stale VM already DELETING (in flight from a previous tick) consumes
	// one slot of the budget of 2, leaving room for only one more deletion.
	inFlight := staleVM("in-flight-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_DELETING, "old-hash", now.Add(-3*time.Hour))
	older := staleVM("stale-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "old-hash", now.Add(-2*time.Hour))
	newer := staleVM("stale-2", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "old-hash", now.Add(-1*time.Hour))

	for _, v := range []*poolmgrv1alpha1.VMRecord{inFlight, older, newer} {
		if err := st.CreateVM(ctx, v); err != nil {
			t.Fatalf("CreateVM(%s): %v", v.GetUid(), err)
		}
	}

	rc := reconciler.NewRolloutController(pool, st, flint, time.Second, nil, metrics.NewRegistry())
	rc.Tick(ctx, now)

	if _, err := st.GetVM(ctx, "stale-1"); err != store.ErrNotFound {
		t.Errorf("expected stale-1 to be deleted, GetVM error = %v", err)
	}
	if _, err := st.GetVM(ctx, "stale-2"); err != nil {
		t.Errorf("expected stale-2 to survive (budget exhausted by in-flight deletion), GetVM error = %v", err)
	}
	if got := vm.deletedUIDs(); len(got) != 1 || got[0] != "stale-1" {
		t.Fatalf("expected exactly 1 DeleteMicroVM(stale-1) call, got %v", got)
	}
}

func TestRolloutController_EmitsPoolRolloutCompletedOnceAtZeroTransition(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 1, []string{"host-a"})
	pool.TemplateHash = "new-hash"
	pool.RolloutPolicy = &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Count{Count: 1}}
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	now := time.Now()
	stale := staleVM("stale-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "old-hash", now.Add(-time.Hour))
	if err := st.CreateVM(ctx, stale); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	rc := reconciler.NewRolloutController(pool, st, flint, time.Second, nil, metrics.NewRegistry())

	// First Tick: the only stale VM is deleted, and stale_count transitions
	// from nonzero to zero within this same Tick, so the completion event
	// fires now.
	rc.Tick(ctx, now)

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	got := eventTypes(t, events)
	if len(got) != 2 || got[0] != poolmgrv1alpha1.EventType_VM_DELETED_FOR_ROLLOUT || got[1] != poolmgrv1alpha1.EventType_POOL_ROLLOUT_COMPLETED {
		t.Fatalf("expected [VM_DELETED_FOR_ROLLOUT, POOL_ROLLOUT_COMPLETED], got %v", got)
	}

	// A further Tick with nothing stale left must not re-emit the
	// completion event.
	rc.Tick(ctx, now.Add(time.Second))

	events, err = st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if got := eventTypes(t, events); len(got) != 2 {
		t.Fatalf("expected no additional events on a subsequent tick, got %v", got)
	}
}

// raceyClaimStore wraps a real store.Store and, on its first
// ListVMsByPool call, claims raceUID out from under the caller via a real
// ClaimAvailableVM - simulating a concurrent ClaimVM winning the race in the
// window between RolloutController.Tick's snapshot read and its later
// EnsureVMDeletedIfPhase call on that same (now stale) snapshot.
type raceyClaimStore struct {
	store.Store
	poolName, poolNamespace string
	triggered               bool
}

func (r *raceyClaimStore) ListVMsByPool(ctx context.Context, poolName, poolNamespace string, phase *poolmgrv1alpha1.VMPhase) ([]*poolmgrv1alpha1.VMRecord, error) {
	vms, err := r.Store.ListVMsByPool(ctx, poolName, poolNamespace, phase)
	if err != nil {
		return nil, err
	}
	if !r.triggered {
		r.triggered = true
		if _, err := r.ClaimAvailableVM(ctx, r.poolName, r.poolNamespace); err != nil {
			panic("raceyClaimStore: test setup: ClaimAvailableVM: " + err.Error())
		}
	}
	return vms, nil
}

func TestRolloutController_SkipsVMConcurrentlyClaimedDuringTick(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	realStore := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 5, []string{"host-a"})
	pool.TemplateHash = "new-hash"
	pool.RolloutPolicy = &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Count{Count: 1}}
	if err := realStore.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	now := time.Now()
	stale := staleVM("stale-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "old-hash", now.Add(-time.Hour))
	if err := realStore.CreateVM(ctx, stale); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	st := &raceyClaimStore{Store: realStore, poolName: "pool-a", poolNamespace: "default"}
	rc := reconciler.NewRolloutController(pool, st, flint, time.Second, nil, metrics.NewRegistry())
	rc.Tick(ctx, now)

	// The VM must survive, and be LEASED (from the simulated concurrent
	// claim), not deleted by the rollout path racing it.
	got, err := realStore.GetVM(ctx, "stale-1")
	if err != nil {
		t.Fatalf("expected stale-1 to survive the race, GetVM error = %v", err)
	}
	if got.GetPhase() != poolmgrv1alpha1.VMPhase_LEASED {
		t.Fatalf("expected stale-1 to be LEASED (claimed), got phase %v", got.GetPhase())
	}

	if got := vm.deletedUIDs(); len(got) != 0 {
		t.Fatalf("expected no DeleteMicroVM calls, got %v", got)
	}

	events, err := realStore.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no events (no deletion happened), got %v", eventTypes(t, events))
	}
}

func TestRolloutController_CountZeroPausesDeletions(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 5, []string{"host-a"})
	pool.TemplateHash = "new-hash"
	pool.RolloutPolicy = &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Count{Count: 0}}
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	now := time.Now()
	stale := staleVM("stale-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "old-hash", now.Add(-time.Hour))
	if err := st.CreateVM(ctx, stale); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	rc := reconciler.NewRolloutController(pool, st, flint, time.Second, nil, metrics.NewRegistry())
	rc.Tick(ctx, now)

	if _, err := st.GetVM(ctx, "stale-1"); err != nil {
		t.Fatalf("expected stale-1 to survive with rollout paused, GetVM error = %v", err)
	}
	if got := vm.deletedUIDs(); len(got) != 0 {
		t.Fatalf("expected no DeleteMicroVM calls with count:0, got %v", got)
	}
}

func TestRolloutController_NoRolloutCompletedWhenNeverStale(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 1, []string{"host-a"})
	pool.TemplateHash = "new-hash"
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	now := time.Now()
	current := staleVM("current-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "new-hash", now)
	if err := st.CreateVM(ctx, current); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	rc := reconciler.NewRolloutController(pool, st, flint, time.Second, nil, metrics.NewRegistry())
	rc.Tick(ctx, now)

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no events when nothing is stale, got %v", eventTypes(t, events))
	}
	if got := vm.deletedUIDs(); len(got) != 0 {
		t.Fatalf("expected no deletions, got %v", got)
	}
}
