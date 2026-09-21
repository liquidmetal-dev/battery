package reconciler_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
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

// newTestRolloutController returns a RolloutController for pool whose
// replacements provision quickly against the fake flintlock, and waits for
// them at test end, before the store is closed.
func newTestRolloutController(t *testing.T, pool *poolmgrv1alpha1.PoolSpec, st store.Store, flint *flintlockclient.Pool) *reconciler.RolloutController {
	t.Helper()
	rc := reconciler.NewRolloutController(pool, st, flint, time.Second, fastProvisionConfig(), metrics.NewRegistry())
	t.Cleanup(rc.Wait)
	return rc
}

// rolloutEventTypes is eventTypes restricted to the events RolloutController
// itself emits, leaving out the VM_PROVISIONED/VM_AVAILABLE events its
// background replacements emit on their own schedule.
func rolloutEventTypes(t *testing.T, st store.Store) []poolmgrv1alpha1.EventType {
	t.Helper()
	events, err := st.ListEventsSince(context.Background(), "pool-a", "default", 0, 1000)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	var out []poolmgrv1alpha1.EventType
	for _, e := range events {
		switch e.GetType() {
		case poolmgrv1alpha1.EventType_VM_DELETED_FOR_ROLLOUT, poolmgrv1alpha1.EventType_POOL_ROLLOUT_COMPLETED:
			out = append(out, e.GetType())
		}
	}
	return out
}

// vmsByHash returns how many of pool-a's VMs are in phase with templateHash.
func vmsByHash(t *testing.T, st store.Store, phase poolmgrv1alpha1.VMPhase, templateHash string) int {
	t.Helper()
	vms, err := st.ListVMsByPool(context.Background(), "pool-a", "default", &phase)
	if err != nil {
		t.Fatalf("ListVMsByPool: %v", err)
	}
	n := 0
	for _, vm := range vms {
		if vm.GetTemplateHash() == templateHash {
			n++
		}
	}
	return n
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

	rc := newTestRolloutController(t, pool, st, flint)
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

	if got := rolloutEventTypes(t, st); len(got) != 2 ||
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

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 3, []string{"host-a"})
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

	rc := newTestRolloutController(t, pool, st, flint)
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

// seedStaleWarmPool creates pool (hash "new-hash") with n stale AVAILABLE VMs,
// oldest first as stale-1..stale-n.
func seedStaleWarmPool(t *testing.T, st store.Store, pool *poolmgrv1alpha1.PoolSpec, n int) {
	t.Helper()
	ctx := context.Background()
	pool.TemplateHash = "new-hash"
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	now := time.Now()
	for i := 1; i <= n; i++ {
		v := staleVM(fmt.Sprintf("stale-%d", i), "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE, "old-hash", now.Add(time.Duration(i-n-1)*time.Hour))
		if err := st.CreateVM(ctx, v); err != nil {
			t.Fatalf("CreateVM(%s): %v", v.GetUid(), err)
		}
	}
}

// The first replacement's create hangs, so it has no store row yet. With
// the default max_unavailable of 1 that pending replacement must still use
// the whole budget: a second Tick deletes nothing, leaving 2 of 3 VMs,
// instead of deleting another healthy VM.
func TestRolloutController_ChargesPendingReplacementAgainstBudget(t *testing.T) {
	gate := make(chan struct{})
	vm := &fakeMicroVM{createGate: gate}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 3, []string{"host-a"})
	seedStaleWarmPool(t, st, pool, 3)
	rc := newTestRolloutController(t, pool, st, flint)
	var openGate sync.Once
	release := func() { openGate.Do(func() { close(gate) }) }
	t.Cleanup(release) // runs before rc.Wait, so a failed assertion can't hang the test
	now := time.Now()

	rc.Tick(ctx, now)
	rc.Tick(ctx, now.Add(time.Second))

	if got := vm.deletedUIDs(); len(got) != 1 || got[0] != "stale-1" {
		t.Fatalf("expected only stale-1 deleted while its replacement is pending, got %v", got)
	}
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 2 {
		t.Fatalf("expected 2 VMs while the replacement is pending, got %d", got)
	}

	// Once the replacement is AVAILABLE, the budget frees up.
	release()
	rc.Wait()
	rc.Tick(ctx, now.Add(2*time.Second))
	if got := vm.deletedUIDs(); len(got) != 2 || got[1] != "stale-2" {
		t.Fatalf("expected stale-2 deleted once the first replacement is AVAILABLE, got %v", got)
	}
}

// A replacement whose Provision fails stays owed: the next Tick retries it
// instead of deleting another stale VM.
func TestRolloutController_RetriesFailedReplacementBeforeDeletingMore(t *testing.T) {
	vm := &fakeMicroVM{failCreatesRemaining: 1}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 3, []string{"host-a"})
	seedStaleWarmPool(t, st, pool, 3)
	rc := newTestRolloutController(t, pool, st, flint)
	now := time.Now()

	rc.Tick(ctx, now) // deletes stale-1; its replacement's create fails
	rc.Wait()
	if got := vmsByHash(t, st, poolmgrv1alpha1.VMPhase_AVAILABLE, "new-hash"); got != 0 {
		t.Fatalf("expected the first replacement to have failed, got %d new VMs", got)
	}

	rc.Tick(ctx, now.Add(time.Second)) // retries the replacement; deletes nothing
	rc.Wait()
	if got := vm.deletedUIDs(); len(got) != 1 {
		t.Fatalf("expected no deletion while a replacement is owed, got %v", got)
	}
	if got := vmsByHash(t, st, poolmgrv1alpha1.VMPhase_AVAILABLE, "new-hash"); got != 1 {
		t.Fatalf("expected the retried replacement to be AVAILABLE, got %d new VMs", got)
	}

	rc.Tick(ctx, now.Add(2*time.Second))
	if got := vm.deletedUIDs(); len(got) != 2 {
		t.Fatalf("expected the next stale VM deleted once the replacement landed, got %v", got)
	}
}

// A controller starting mid-rollout (e.g. after a restart) can't know
// about its predecessor's owed replacements, so it charges the pool's
// deficit instead: here stale-1 was deleted and its replacement lost, so
// the first Tick replaces it rather than deleting another VM.
func TestRolloutController_ResumesByReplacingDeficitBeforeDeleting(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE, 3, []string{"host-a"})
	seedStaleWarmPool(t, st, pool, 2)
	rc := newTestRolloutController(t, pool, st, flint)

	rc.Tick(ctx, time.Now())
	rc.Wait()

	if got := vm.deletedUIDs(); len(got) != 0 {
		t.Fatalf("expected no deletion on a first Tick with a deficit, got %v", got)
	}
	if got := vmsByHash(t, st, poolmgrv1alpha1.VMPhase_AVAILABLE, "new-hash"); got != 1 {
		t.Fatalf("expected the deficit replaced by 1 new VM, got %d", got)
	}
}

// End to end with the pool's real Reconciler running alongside: a warm pool
// of stale VMs converges to size current VMs whatever its strategy - none
// of which replenish on a rollout deletion by themselves (and
// REPLACE_ON_DELETE, which does on other deletions, mustn't also do so here).
func TestRolloutController_ConvergesWithReconcilerForEveryStrategy(t *testing.T) {
	for _, strategy := range []poolmgrv1alpha1.ReplenishmentStrategyType{
		poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE,
		poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD,
		poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE,
	} {
		t.Run(strategy.String(), func(t *testing.T) {
			vm := &fakeMicroVM{}
			flint := startFakeFlintlock(t, vm, alwaysReadyExec())
			st := openTestStore(t)

			pool := samplePool("pool-a", strategy, 3, []string{"host-a"})
			if strategy == poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD {
				pool.ReplenishmentStrategy.MinSize = int32Ptr(1)
			}
			seedStaleWarmPool(t, st, pool, 3)

			r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			rc := reconciler.NewRolloutController(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); _ = r.Run(ctx) }()
			go func() { defer wg.Done(); _ = rc.Run(ctx) }()
			t.Cleanup(func() { cancel(); wg.Wait() })

			deadline := time.Now().Add(5 * time.Second)
			for vmsByHash(t, st, poolmgrv1alpha1.VMPhase_AVAILABLE, "new-hash") < 3 {
				if time.Now().After(deadline) {
					t.Fatalf("timed out converging: %d new AVAILABLE VMs", vmsByHash(t, st, poolmgrv1alpha1.VMPhase_AVAILABLE, "new-hash"))
				}
				time.Sleep(10 * time.Millisecond)
			}
			// Let any duplicate replacement land before counting.
			time.Sleep(100 * time.Millisecond)

			if got := len(onlyVMsInPool(t, st, "pool-a")); got != 3 {
				t.Fatalf("expected exactly 3 VMs after the rollout, got %d", got)
			}
			if got := rolloutEventTypes(t, st); len(got) != 4 || got[3] != poolmgrv1alpha1.EventType_POOL_ROLLOUT_COMPLETED {
				t.Fatalf("expected 3 VM_DELETED_FOR_ROLLOUT then POOL_ROLLOUT_COMPLETED, got %v", got)
			}
		})
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

	rc := newTestRolloutController(t, pool, st, flint)

	// First Tick: the only stale VM is deleted, but its replacement is
	// still owed, so the rollout isn't complete yet.
	rc.Tick(ctx, now)
	if got := rolloutEventTypes(t, st); len(got) != 1 || got[0] != poolmgrv1alpha1.EventType_VM_DELETED_FOR_ROLLOUT {
		t.Fatalf("expected [VM_DELETED_FOR_ROLLOUT] before the replacement is AVAILABLE, got %v", got)
	}

	// Once the replacement is AVAILABLE, the next Tick completes the
	// rollout.
	rc.Wait()
	rc.Tick(ctx, now.Add(time.Second))
	got := rolloutEventTypes(t, st)
	if len(got) != 2 || got[0] != poolmgrv1alpha1.EventType_VM_DELETED_FOR_ROLLOUT || got[1] != poolmgrv1alpha1.EventType_POOL_ROLLOUT_COMPLETED {
		t.Fatalf("expected [VM_DELETED_FOR_ROLLOUT, POOL_ROLLOUT_COMPLETED], got %v", got)
	}

	// A further Tick with nothing stale left must not re-emit the
	// completion event.
	rc.Tick(ctx, now.Add(2*time.Second))
	if got := rolloutEventTypes(t, st); len(got) != 2 {
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

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 1, []string{"host-a"})
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
	rc := newTestRolloutController(t, pool, st, flint)
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

	rc := newTestRolloutController(t, pool, st, flint)
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

	rc := newTestRolloutController(t, pool, st, flint)
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
