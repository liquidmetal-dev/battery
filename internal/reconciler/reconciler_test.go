package reconciler_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func alwaysReadyExec() *fakeMicroVMExec {
	return &fakeMicroVMExec{
		respond: func(*microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			return nil, nil, 0, "", nil
		},
	}
}

// waitForVMs polls until the pool has at least want VMs (of any phase), or
// fails the test after timeout.
func waitForVMs(t *testing.T, st store.Store, poolName string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(onlyVMsInPool(t, st, poolName)) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d VM(s) in pool %q", want, poolName)
}

func int32Ptr(v int32) *int32 { return &v }

func TestReconciler_MinSizeThreshold_TickDrivenTopUp(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	seedHost(t, st, "host-a")

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 2, []string{"host-a"})
	pool.ReplenishmentStrategy.MinSize = int32Ptr(2)

	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	waitForVMs(t, st, "pool-a", 2, 2*time.Second)

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestReconciler_MinSizeThreshold_CountsPreLeaseHookRunningAsInFlight(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	seedHost(t, st, "host-a")
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 2, []string{"host-a"})
	pool.ReplenishmentStrategy.MinSize = int32Ptr(2)
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	// Already at the pool's target size (2): one AVAILABLE, one
	// PRE_LEASE_HOOK_RUNNING (about to be claimed). If PRE_LEASE_HOOK_RUNNING
	// isn't counted as in-flight, the reconciler will wrongly provision a
	// third VM.
	for _, v := range []*poolmgrv1alpha1.VMRecord{
		sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE),
		sampleVM("vm-2", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING),
	} {
		if err := st.CreateVM(ctx, v); err != nil {
			t.Fatalf("CreateVM(%s): %v", v.GetUid(), err)
		}
	}

	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(runCtx) }()

	// Give several ticks a chance to (wrongly) over-provision.
	time.Sleep(100 * time.Millisecond)

	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 2 {
		t.Fatalf("expected pool to stay at 2 VMs, got %d", got)
	}
}

// seedAvailableVMs creates the pool in st along with n AVAILABLE VMs named
// seed-0..seed-(n-1), so a starting reconciler has nothing to provision. The
// uids deliberately don't use the fake flintlock's "vm-N" scheme: its
// counter is shared by the whole test binary, so a test run on its own
// would otherwise be handed a uid that is already seeded.
func seedAvailableVMs(t *testing.T, st store.Store, pool *poolmgrv1alpha1.PoolSpec, n int) {
	t.Helper()
	ctx := context.Background()
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	for i := range n {
		v := sampleVM(fmt.Sprintf("seed-%d", i), pool.GetName(), "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE)
		if err := st.CreateVM(ctx, v); err != nil {
			t.Fatalf("CreateVM(%s): %v", v.GetUid(), err)
		}
	}
}

// waitForAvailable polls until the pool has exactly want AVAILABLE VMs, or
// fails the test after timeout.
func waitForAvailable(t *testing.T, st store.Store, poolName string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := 0
		for _, v := range onlyVMsInPool(t, st, poolName) {
			if v.GetPhase() == poolmgrv1alpha1.VMPhase_AVAILABLE {
				got++
			}
		}
		if got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d AVAILABLE VM(s) in pool %q", want, poolName)
}

var eventDrivenStrategies = []poolmgrv1alpha1.ReplenishmentStrategyType{
	poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE,
	poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE,
}

func TestReconciler_EventDriven_FillsFreshPool(t *testing.T) {
	for _, strategy := range eventDrivenStrategies {
		t.Run(strategy.String(), func(t *testing.T) {
			vm := &fakeMicroVM{}
			flint := startFakeFlintlock(t, vm, alwaysReadyExec())
			st := openTestStore(t)
			seedHost(t, st, "host-a")

			// A fresh pool with no VMs: nothing can be claimed or deleted,
			// so it has to fill without waiting for a notification.
			pool := samplePool("pool-a", strategy, 3, []string{"host-a"})

			r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = r.Run(ctx) }()

			waitForVMs(t, st, "pool-a", 3, 2*time.Second)

			// Give several ticks a chance to (wrongly) over-provision.
			time.Sleep(50 * time.Millisecond)
			if got := len(onlyVMsInPool(t, st, "pool-a")); got != 3 {
				t.Fatalf("expected pool to stay at 3 VMs once full, got %d", got)
			}
		})
	}
}

// A provision that fails must not leave the pool short forever: with the
// pool's only VM gone there is nothing to claim or delete, so only a later
// tick can make up the difference.
func TestReconciler_EventDriven_RecoversAfterFailedProvision(t *testing.T) {
	for _, strategy := range eventDrivenStrategies {
		t.Run(strategy.String(), func(t *testing.T) {
			var setupCalls atomic.Int64
			exec := &fakeMicroVMExec{
				respond: func(start *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
					if start.GetCmd() == "setup" && setupCalls.Add(1) == 1 {
						return nil, nil, 1, "", nil
					}
					return nil, nil, 0, "", nil
				},
			}
			vm := &fakeMicroVM{}
			flint := startFakeFlintlock(t, vm, exec)
			st := openTestStore(t)
			seedHost(t, st, "host-a")

			pool := samplePool("pool-a", strategy, 1, []string{"host-a"})
			pool.CreateCommands = []string{"setup"}
			pool.HookFailurePolicy = poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE

			r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = r.Run(ctx) }()

			waitForAvailable(t, st, "pool-a", 1, 2*time.Second)

			time.Sleep(50 * time.Millisecond)
			if got := len(onlyVMsInPool(t, st, "pool-a")); got != 1 {
				t.Fatalf("expected pool to stay at 1 VM after recovering, got %d", got)
			}
		})
	}
}

// longTick keeps the tick out of a test that is about what a notification
// alone does.
const longTick = time.Hour

func TestReconciler_ImmediateOnLease_ClaimNotificationReplenishes(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	seedHost(t, st, "host-a")

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE, 2, []string{"host-a"})
	seedAvailableVMs(t, st, pool, 2)

	r, err := reconciler.New(pool, st, flint, longTick, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	// Already at size, so starting up should provision nothing.
	time.Sleep(50 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 2 {
		t.Fatalf("expected 2 VMs before any claim, got %d", got)
	}

	leaseVM(t, st, "seed-0", "pool-a")
	r.NotifyVMClaimed()
	waitForAvailable(t, st, "pool-a", 2, 2*time.Second)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 3 {
		t.Fatalf("expected 3 VMs (1 leased, 2 warm) after the claim, got %d", got)
	}
}

func TestReconciler_ReplaceOnDelete_DeleteNotificationReplenishes(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	seedHost(t, st, "host-a")

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 3, []string{"host-a"})
	seedAvailableVMs(t, st, pool, 3)

	r, err := reconciler.New(pool, st, flint, longTick, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 3 {
		t.Fatalf("expected 3 VMs before any delete, got %d", got)
	}

	// Two deletions that reach the reconciler as a single notification, as
	// happens when the second lands while the first is still pending: both
	// have to be replaced.
	for _, uid := range []string{"seed-0", "seed-1"} {
		if err := st.DeleteVM(context.Background(), uid); err != nil {
			t.Fatalf("DeleteVM(%s): %v", uid, err)
		}
	}
	r.NotifyVMDeleted()
	waitForAvailable(t, st, "pool-a", 3, 2*time.Second)
}

// A tick that runs between a claim and its notification already replaces the
// claimed VM; the notification must not then add a second replacement.
func TestReconciler_ImmediateOnLease_TickAndNotificationDoNotOvershoot(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	seedHost(t, st, "host-a")

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE, 2, []string{"host-a"})
	seedAvailableVMs(t, st, pool, 2)

	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	leaseVM(t, st, "seed-0", "pool-a")
	waitForAvailable(t, st, "pool-a", 2, 2*time.Second)
	r.NotifyVMClaimed()

	time.Sleep(50 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 3 {
		t.Fatalf("expected 3 VMs (1 leased, 2 warm), got %d", got)
	}
}

// A claim that hasn't committed its lease yet can still be handed back (see
// api.LeaseServer.yieldClaim), so it must keep counting toward the warm set:
// replacing it early would leave a surplus VM when it returns to AVAILABLE.
func TestReconciler_ImmediateOnLease_PendingClaimIsNotReplaced(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	seedHost(t, st, "host-a")
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE, 2, []string{"host-a"})
	seedAvailableVMs(t, st, pool, 2)

	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(runCtx) }()

	// Two ClaimVM calls racing on one request_id each reserve a VM: one is
	// still running its pre-lease hook. The other carries a lease id with
	// no lease behind it, which the claim path no longer produces but
	// CountVMs must still read as pending rather than trust the id.
	hookRunning := sampleVM("seed-0", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING)
	uncommitted := sampleVM("seed-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_LEASED)
	uncommittedLease := "lease-never-created"
	uncommitted.LeaseId = &uncommittedLease
	for _, v := range []*poolmgrv1alpha1.VMRecord{hookRunning, uncommitted} {
		if err := st.UpdateVM(ctx, v); err != nil {
			t.Fatalf("UpdateVM(%s): %v", v.GetUid(), err)
		}
	}

	// Give several ticks a chance to (wrongly) replace the pending claims.
	time.Sleep(50 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 2 {
		t.Fatalf("expected no replacement while both claims are pending, got %d VMs", got)
	}

	// One claim wins and commits its lease; the loser yields its VM.
	leaseVM(t, st, "seed-0", "pool-a")
	if err := st.UpdateVM(ctx, sampleVM("seed-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("UpdateVM(seed-1): %v", err)
	}
	r.NotifyVMClaimed()

	waitForAvailable(t, st, "pool-a", 2, 2*time.Second)
	time.Sleep(50 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 3 {
		t.Fatalf("expected 3 VMs (1 leased, 2 warm), got %d", got)
	}
}

// leaseVM flips an existing VM to LEASED and creates its lease, standing in
// for a ClaimVM that has committed.
func leaseVM(t *testing.T, st store.Store, uid, poolName string) {
	t.Helper()
	ctx := context.Background()
	vm := sampleVM(uid, poolName, "host-a", poolmgrv1alpha1.VMPhase_LEASED)
	leaseID := "lease-" + uid
	vm.LeaseId = &leaseID
	if err := st.UpdateVM(ctx, vm); err != nil {
		t.Fatalf("UpdateVM(%s): %v", uid, err)
	}
	now := timestamppb.Now()
	lease := &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         leaseID,
		VmUid:           uid,
		PoolName:        poolName,
		PoolNamespace:   "default",
		ClaimedAt:       now,
		LastHeartbeatAt: now,
		ExpiresAt:       timestamppb.New(now.AsTime().Add(time.Hour)),
	}
	if err := st.CreateLease(ctx, lease); err != nil {
		t.Fatalf("CreateLease(%s): %v", leaseID, err)
	}
}

// staticPool returns a size-1 pool whose template gives its interface a
// static address, which every VM provisioned from it would share.
func staticPool(strategy poolmgrv1alpha1.ReplenishmentStrategyType) *poolmgrv1alpha1.PoolSpec {
	pool := samplePool("pool-a", strategy, 1, []string{"host-a"})
	if strategy == poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD {
		pool.ReplenishmentStrategy.MinSize = int32Ptr(1)
	}
	pool.MicrovmTemplate.Interfaces = []*flintlocktypes.NetworkInterface{{
		DeviceId: "eth1",
		Address:  &flintlocktypes.StaticAddress{Address: "192.168.100.31/32"},
	}}
	return pool
}

// TestReconciler_StaticNetwork_WaitsForEveryVMToGo covers the phases no
// Strategy counts: a VM that is DELETING, QUARANTINED or FAILED may still be
// running with the template's address, so a static-network pool must not
// provision alongside it.
func TestReconciler_StaticNetwork_WaitsForEveryVMToGo(t *testing.T) {
	for _, phase := range []poolmgrv1alpha1.VMPhase{
		poolmgrv1alpha1.VMPhase_DELETING,
		poolmgrv1alpha1.VMPhase_QUARANTINED,
		poolmgrv1alpha1.VMPhase_FAILED,
	} {
		for _, strategy := range []poolmgrv1alpha1.ReplenishmentStrategyType{
			poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD,
			poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE,
		} {
			t.Run(phase.String()+"/"+strategy.String(), func(t *testing.T) {
				vm := &fakeMicroVM{}
				flint := startFakeFlintlock(t, vm, alwaysReadyExec())
				st := openTestStore(t)
				seedHost(t, st, "host-a")
				ctx := context.Background()

				pool := staticPool(strategy)
				if err := st.CreatePool(ctx, pool); err != nil {
					t.Fatalf("CreatePool: %v", err)
				}
				if err := st.CreateVM(ctx, sampleVM("vm-old", "pool-a", "host-a", phase)); err != nil {
					t.Fatalf("CreateVM: %v", err)
				}

				r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				runCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go func() { _ = r.Run(runCtx) }()

				// The start-up reconcile and several ticks all see the old VM.
				time.Sleep(100 * time.Millisecond)
				if got := len(onlyVMsInPool(t, st, "pool-a")); got != 1 {
					t.Fatalf("expected no VM provisioned alongside the %v one, got %d VMs", phase, got)
				}

				// Once it is gone the pool replenishes as usual.
				if err := st.DeleteVM(ctx, "vm-old"); err != nil {
					t.Fatalf("DeleteVM: %v", err)
				}
				r.NotifyVMDeleted()
				waitForVMs(t, st, "pool-a", 1, 2*time.Second)
			})
		}
	}
}

// TestReconciler_StaticNetwork_ShrunkPoolReplacesOnlyTheLastVM covers a
// REPLACE_ON_DELETE pool updated from several VMs down to size 1 with a
// static template: no VM may be provisioned while one of the old ones
// remains, and exactly one once the last is gone.
func TestReconciler_StaticNetwork_ShrunkPoolReplacesOnlyTheLastVM(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	seedHost(t, st, "host-a")
	ctx := context.Background()

	pool := staticPool(poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE)
	seedAvailableVMs(t, st, pool, 2)

	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(runCtx) }()

	if err := st.DeleteVM(ctx, "seed-0"); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	r.NotifyVMDeleted()
	time.Sleep(100 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 1 {
		t.Fatalf("expected no replacement while another VM remains, got %d VMs", got)
	}

	if err := st.DeleteVM(ctx, "seed-1"); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	r.NotifyVMDeleted()
	waitForVMs(t, st, "pool-a", 1, 2*time.Second)
	time.Sleep(50 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 1 {
		t.Fatalf("expected exactly one replacement, got %d VMs", got)
	}
}

// TestReconciler_StaticNetwork_NeverProvisionsMoreThanOne covers a pool
// stored before CreatePool rejected a static template at size > 1: the
// reconciler still keeps it to a single VM.
func TestReconciler_StaticNetwork_NeverProvisionsMoreThanOne(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
	seedHost(t, st, "host-a")

	pool := staticPool(poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD)
	pool.Size = 3
	pool.ReplenishmentStrategy.MinSize = int32Ptr(3)

	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	waitForVMs(t, st, "pool-a", 1, 2*time.Second)
	time.Sleep(100 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 1 {
		t.Fatalf("expected the pool to stay at 1 VM, got %d", got)
	}
}

// abandonedClaims are the states a VM can be left in by a poolmgrd that died
// part-way through a ClaimVM, or after the sweeper ended its lease but
// before it marked the VM for deletion: claimed, with no lease to show for it.
func abandonedClaims() map[string]*poolmgrv1alpha1.VMRecord {
	hookRunning := sampleVM("seed-0", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING)
	reserved := sampleVM("seed-0", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_LEASED)
	leaseGone := sampleVM("seed-0", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_LEASED)
	leaseID := "lease-with-no-row"
	leaseGone.LeaseId = &leaseID
	return map[string]*poolmgrv1alpha1.VMRecord{
		"pre-lease hook running":              hookRunning,
		"leased with no lease id":             reserved,
		"leased with a lease id but no lease": leaseGone,
	}
}

// A VM whose claim was abandoned counts as a pending claim, so without
// recovery an IMMEDIATE_ON_LEASE pool restarted with one would treat it as
// warm forever and never provision a VM anyone can claim.
func TestRecoverAbandonedClaims_RestartedPoolRefills(t *testing.T) {
	for name, abandoned := range abandonedClaims() {
		t.Run(name, func(t *testing.T) {
			vm := &fakeMicroVM{}
			flint := startFakeFlintlock(t, vm, alwaysReadyExec())
			st := openTestStore(t)
			seedHost(t, st, "host-a")
			ctx := context.Background()

			pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE, 1, []string{"host-a"})
			if err := st.CreatePool(ctx, pool); err != nil {
				t.Fatalf("CreatePool: %v", err)
			}
			if err := st.CreateVM(ctx, abandoned); err != nil {
				t.Fatalf("CreateVM: %v", err)
			}

			// What poolmgrd does on startup, before any reconciler runs.
			n, err := reconciler.RecoverAbandonedClaims(ctx, st)
			if err != nil {
				t.Fatalf("RecoverAbandonedClaims: %v", err)
			}
			if n != 1 {
				t.Fatalf("RecoverAbandonedClaims() = %d, want 1", n)
			}
			got, err := st.GetVM(ctx, "seed-0")
			if err != nil {
				t.Fatalf("GetVM: %v", err)
			}
			if got.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
				t.Fatalf("abandoned VM phase = %v, want DELETING", got.GetPhase())
			}

			r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			runCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = r.Run(runCtx) }()

			waitForAvailable(t, st, "pool-a", 1, 2*time.Second)
		})
	}
}

func TestRecoverAbandonedClaims_LeavesOtherVMsAlone(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE, 2, []string{"host-a"})
	seedAvailableVMs(t, st, pool, 2)
	leaseVM(t, st, "seed-0", "pool-a")
	for uid, phase := range map[string]poolmgrv1alpha1.VMPhase{
		"provisioning": poolmgrv1alpha1.VMPhase_PROVISIONING,
		"quarantined":  poolmgrv1alpha1.VMPhase_QUARANTINED,
	} {
		if err := st.CreateVM(ctx, sampleVM(uid, "pool-a", "host-a", phase)); err != nil {
			t.Fatalf("CreateVM(%s): %v", uid, err)
		}
	}

	n, err := reconciler.RecoverAbandonedClaims(ctx, st)
	if err != nil {
		t.Fatalf("RecoverAbandonedClaims: %v", err)
	}
	if n != 0 {
		t.Fatalf("RecoverAbandonedClaims() = %d, want 0", n)
	}
	want := map[string]poolmgrv1alpha1.VMPhase{
		"seed-0":       poolmgrv1alpha1.VMPhase_LEASED,
		"seed-1":       poolmgrv1alpha1.VMPhase_AVAILABLE,
		"provisioning": poolmgrv1alpha1.VMPhase_PROVISIONING,
		"quarantined":  poolmgrv1alpha1.VMPhase_QUARANTINED,
	}
	for _, v := range onlyVMsInPool(t, st, "pool-a") {
		if v.GetPhase() != want[v.GetUid()] {
			t.Errorf("VM %s phase = %v, want %v", v.GetUid(), v.GetPhase(), want[v.GetUid()])
		}
	}
}
