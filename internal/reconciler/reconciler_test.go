package reconciler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"

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

func TestReconciler_ImmediateOnLease_OnlyOnClaimNotification(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE, 5, []string{"host-a"})

	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	// Give a few ticks a chance to (wrongly) provision before any claim.
	time.Sleep(50 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 0 {
		t.Fatalf("expected 0 VMs before any claim notification, got %d", got)
	}

	r.NotifyVMClaimed()
	waitForVMs(t, st, "pool-a", 1, 2*time.Second)
}

func TestReconciler_ReplaceOnDelete_OnlyOnDeleteNotification(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 5, []string{"host-a"})

	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	if got := len(onlyVMsInPool(t, st, "pool-a")); got != 0 {
		t.Fatalf("expected 0 VMs before any delete notification, got %d", got)
	}

	r.NotifyVMDeleted()
	waitForVMs(t, st, "pool-a", 1, 2*time.Second)
}
