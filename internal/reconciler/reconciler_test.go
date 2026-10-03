package reconciler_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"

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

// seedAvailableVMs creates the pool in st along with n AVAILABLE VMs, so a
// starting reconciler's seed has nothing left to do.
func seedAvailableVMs(t *testing.T, st store.Store, pool *poolmgrv1alpha1.PoolSpec, n int) {
	t.Helper()
	ctx := context.Background()
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	for i := range n {
		v := sampleVM(fmt.Sprintf("vm-%d", i), pool.GetName(), "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE)
		if err := st.CreateVM(ctx, v); err != nil {
			t.Fatalf("CreateVM(%s): %v", v.GetUid(), err)
		}
	}
}

func TestReconciler_EventDriven_SeedsFreshPool(t *testing.T) {
	for _, strategy := range []poolmgrv1alpha1.ReplenishmentStrategyType{
		poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE,
		poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE,
	} {
		t.Run(strategy.String(), func(t *testing.T) {
			vm := &fakeMicroVM{}
			flint := startFakeFlintlock(t, vm, alwaysReadyExec())
			st := openTestStore(t)

			// A fresh pool with no VMs: without seeding, nothing can be
			// claimed or deleted, so replenishment would never trigger.
			pool := samplePool("pool-a", strategy, 3, []string{"host-a"})

			r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = r.Run(ctx) }()

			waitForVMs(t, st, "pool-a", 3, 2*time.Second)

			// Give several ticks a chance to (wrongly) seed again.
			time.Sleep(50 * time.Millisecond)
			if got := len(onlyVMsInPool(t, st, "pool-a")); got != 3 {
				t.Fatalf("expected pool to stay at 3 VMs after seeding, got %d", got)
			}
		})
	}
}

func TestReconciler_ImmediateOnLease_OnlyOnClaimNotification(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE, 2, []string{"host-a"})
	seedAvailableVMs(t, st, pool, 2)

	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	// Already at size, so neither the seed nor any tick should provision
	// before a claim.
	time.Sleep(50 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 2 {
		t.Fatalf("expected 2 VMs before any claim notification, got %d", got)
	}

	r.NotifyVMClaimed()
	waitForVMs(t, st, "pool-a", 3, 2*time.Second)
}

func TestReconciler_ReplaceOnDelete_OnlyOnDeleteNotification(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 2, []string{"host-a"})
	seedAvailableVMs(t, st, pool, 2)

	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 2 {
		t.Fatalf("expected 2 VMs before any delete notification, got %d", got)
	}

	r.NotifyVMDeleted()
	waitForVMs(t, st, "pool-a", 3, 2*time.Second)
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

				// The seed and several ticks all see the old VM.
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
// static template: each old VM's deletion asks for one replacement, but only
// the last one may get it.
func TestReconciler_StaticNetwork_ShrunkPoolReplacesOnlyTheLastVM(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)
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

	if err := st.DeleteVM(ctx, "vm-0"); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	r.NotifyVMDeleted()
	time.Sleep(100 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 1 {
		t.Fatalf("expected no replacement while another VM remains, got %d VMs", got)
	}

	if err := st.DeleteVM(ctx, "vm-1"); err != nil {
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
