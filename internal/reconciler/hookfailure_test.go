package reconciler_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// failFirstSetupExec answers every exec with exit code 0, except the first
// "setup" command, which exits 1: the first VM's create hook fails and every
// later VM's passes.
func failFirstSetupExec() *fakeMicroVMExec {
	var setupCalls atomic.Int64
	return &fakeMicroVMExec{
		respond: func(start *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			if start.GetCmd() == "setup" && setupCalls.Add(1) == 1 {
				return nil, nil, 1, "", nil
			}
			return nil, nil, 0, "", nil
		},
	}
}

// assertPoolWantsReplacement fails t unless pool's counts leave the VM it
// lost out (nothing provisioning, nothing available) and its strategy asks
// for one new VM.
func assertPoolWantsReplacement(t *testing.T, st store.Store, pool *poolmgrv1alpha1.PoolSpec) {
	t.Helper()
	counts, err := reconciler.CountVMs(context.Background(), st, pool.GetName(), pool.GetNamespace())
	if err != nil {
		t.Fatalf("CountVMs: %v", err)
	}
	if counts != (reconciler.VMCounts{}) {
		t.Fatalf("CountVMs() = %+v, want all zero: a DELETING VM must not count toward the pool", counts)
	}
	strategy, err := reconciler.NewStrategy(pool.GetReplenishmentStrategy())
	if err != nil {
		t.Fatalf("NewStrategy: %v", err)
	}
	if got := strategy.DesiredNewVMs(pool, counts); got != 1 {
		t.Fatalf("DesiredNewVMs() = %d, want 1", got)
	}
}

// runReconcilerUntilAvailable runs a Reconciler for pool until it has one
// AVAILABLE VM, then stops it.
func runReconcilerUntilAvailable(t *testing.T, st store.Store, flint *flintlockclient.Pool, pool *poolmgrv1alpha1.PoolSpec) {
	t.Helper()
	r, err := reconciler.New(pool, st, flint, 10*time.Millisecond, fastProvisionConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitForAvailable(t, st, pool.GetName(), 1, 2*time.Second)
	cancel()
	<-done
}

// assertSweeperFinishesDelete runs one Sweeper tick and fails t unless it
// deleted microvm uid through flintlock, removed its row and told the pool.
func assertSweeperFinishesDelete(t *testing.T, st store.Store, flint *flintlockclient.Pool, fake *fakeMicroVM, uid string) {
	t.Helper()
	ctx := context.Background()
	notifier := &spyNotifier{}
	reconciler.NewSweeper(st, flint, time.Hour, 0, notifier, nil).Tick(ctx, time.Now())

	if _, err := st.GetVM(ctx, uid); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("after a sweeper tick, GetVM(%s) error = %v, want ErrNotFound", uid, err)
	}
	if !slices.Contains(fake.deletedUIDs(), uid) {
		t.Fatalf("after a sweeper tick, flintlock deleted %v, want %s among them", fake.deletedUIDs(), uid)
	}
	if !slices.Equal(notifier.deleted, []string{"default/pool-a"}) {
		t.Fatalf("sweeper notifications = %v, want one for default/pool-a", notifier.deleted)
	}
}

// TestProvision_HookFailureDeleteNotConfirmed_LeftDeletingAndReplaced: when
// a create hook fails under DELETE_AND_REPLACE and flintlock doesn't confirm
// the delete (it fails, or hangs past the cleanup timeout), the VM must stay
// DELETING: not dropped (the microvm would leak) and not left PROVISIONING
// (the pool would count it for good). The pool replaces it at once, and the
// Sweeper deletes it once flintlock answers.
func TestProvision_HookFailureDeleteNotConfirmed_LeftDeletingAndReplaced(t *testing.T) {
	tests := []struct {
		name string
		fake func(t *testing.T) *fakeMicroVM
	}{
		{"delete fails", func(*testing.T) *fakeMicroVM {
			return &fakeMicroVM{failDeletesRemaining: 1}
		}},
		{"delete hangs past the cleanup timeout", func(t *testing.T) *fakeMicroVM {
			reconciler.SetCleanupTimeoutsForTest(t, 200*time.Millisecond)
			return &fakeMicroVM{hangDeletesRemaining: 1}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := tt.fake(t)
			flint := startFakeFlintlock(t, fake, failFirstSetupExec())
			st := openTestStore(t)
			ctx := context.Background()

			pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 1, []string{"host-a"})
			pool.CreateCommands = []string{"setup"}
			pool.HookFailurePolicy = poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE
			if err := st.CreatePool(ctx, pool); err != nil {
				t.Fatalf("CreatePool: %v", err)
			}

			p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)
			if err := p.Provision(ctx, pool); !errors.Is(err, reconciler.ErrHookFailed) {
				t.Fatalf("Provision() error = %v, want ErrHookFailed", err)
			}

			vms := onlyVMsInPool(t, st, "pool-a")
			if len(vms) != 1 || vms[0].GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
				t.Fatalf("after the hook failure, VMs = %+v, want one DELETING", vms)
			}
			uid := vms[0].GetUid()
			if got := fake.deletedUIDs(); len(got) != 0 {
				t.Fatalf("flintlock deleted %v, want none yet", got)
			}
			events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
			if err != nil {
				t.Fatalf("ListEventsSince: %v", err)
			}
			if !slices.Contains(eventTypes(t, events), poolmgrv1alpha1.EventType_VM_HOOK_FAILED) {
				t.Fatalf("events = %v, want VM_HOOK_FAILED among them", eventTypes(t, events))
			}

			assertPoolWantsReplacement(t, st, pool)
			runReconcilerUntilAvailable(t, st, flint, pool)

			assertSweeperFinishesDelete(t, st, flint, fake, uid)
		})
	}
}

// TestProvision_OrphanDeleteFails_LeftDeletingForSweeper: flintlock created
// the microvm but its CreateVM store write failed, and the first delete of
// the orphan fails. The orphan must be recorded DELETING so the Sweeper
// deletes it later, rather than forgotten while it runs on.
func TestProvision_OrphanDeleteFails_LeftDeletingForSweeper(t *testing.T) {
	fake := &fakeMicroVM{failDeletesRemaining: 1}
	flint := startFakeFlintlock(t, fake, alwaysReadyExec())
	st := &failingStore{Store: openTestStore(t), failCreateVMPhase: vmPhasePtr(poolmgrv1alpha1.VMPhase_PROVISIONING)}
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 1, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)
	if err := p.Provision(ctx, pool); !errors.Is(err, errInjected) {
		t.Fatalf("Provision() error = %v, want wrapped errInjected", err)
	}

	vms := onlyVMsInPool(t, st, "pool-a")
	if len(vms) != 1 || vms[0].GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Fatalf("after the failed store write, VMs = %+v, want the orphan recorded DELETING", vms)
	}
	if got := fake.deletedUIDs(); len(got) != 0 {
		t.Fatalf("flintlock deleted %v, want none yet", got)
	}
	assertPoolWantsReplacement(t, st, pool)

	assertSweeperFinishesDelete(t, st, flint, fake, vms[0].GetUid())
}

// TestRecoverAbandonedProvisions_RestartedPoolRefills: a VM left PROVISIONING
// or CREATE_HOOK_RUNNING by a previous process counts as provisioning, so
// without recovery its pool would never replace it.
func TestRecoverAbandonedProvisions_RestartedPoolRefills(t *testing.T) {
	fake := &fakeMicroVM{}
	flint := startFakeFlintlock(t, fake, alwaysReadyExec())
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 3, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	for _, vm := range []*poolmgrv1alpha1.VMRecord{
		sampleVM("vm-provisioning", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_PROVISIONING),
		sampleVM("vm-hook", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING),
		sampleVM("vm-available", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE),
	} {
		if err := st.CreateVM(ctx, vm); err != nil {
			t.Fatalf("CreateVM(%s): %v", vm.GetUid(), err)
		}
	}

	n, err := reconciler.RecoverAbandonedProvisions(ctx, st)
	if err != nil {
		t.Fatalf("RecoverAbandonedProvisions: %v", err)
	}
	if n != 2 {
		t.Fatalf("RecoverAbandonedProvisions() = %d, want 2", n)
	}
	for uid, want := range map[string]poolmgrv1alpha1.VMPhase{
		"vm-provisioning": poolmgrv1alpha1.VMPhase_DELETING,
		"vm-hook":         poolmgrv1alpha1.VMPhase_DELETING,
		"vm-available":    poolmgrv1alpha1.VMPhase_AVAILABLE,
	} {
		got, err := st.GetVM(ctx, uid)
		if err != nil {
			t.Fatalf("GetVM(%s): %v", uid, err)
		}
		if got.GetPhase() != want {
			t.Fatalf("VM %s phase = %v, want %v", uid, got.GetPhase(), want)
		}
	}

	counts, err := reconciler.CountVMs(ctx, st, "pool-a", "default")
	if err != nil {
		t.Fatalf("CountVMs: %v", err)
	}
	if counts != (reconciler.VMCounts{Available: 1}) {
		t.Fatalf("CountVMs() = %+v, want only the AVAILABLE VM counted", counts)
	}

	reconciler.NewSweeper(st, flint, time.Hour, 0, nil, nil).Tick(ctx, time.Now())
	if got := fake.deletedUIDs(); !slices.Equal(slices.Sorted(slices.Values(got)), []string{"vm-hook", "vm-provisioning"}) {
		t.Fatalf("flintlock deleted %v, want vm-hook and vm-provisioning", got)
	}
}
