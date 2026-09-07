package reconciler_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func fastProvisionConfig() reconciler.ProvisionConfig {
	return reconciler.ProvisionConfig{
		CreatePollInterval: 5 * time.Millisecond,
		CreatePollTimeout:  2 * time.Second,
		GuestAgentInterval: 5 * time.Millisecond,
		GuestAgentTimeout:  2 * time.Second,
	}
}

func onlyVMsInPool(t *testing.T, st store.Store, poolName string) []*poolmgrv1alpha1.VMRecord {
	t.Helper()
	vms, err := st.ListVMsByPool(context.Background(), poolName, "default", nil)
	if err != nil {
		t.Fatalf("ListVMsByPool: %v", err)
	}
	return vms
}

func TestProvision_HappyPath(t *testing.T) {
	vm := &fakeMicroVM{pollsUntilCreated: 2}
	exec := alwaysReadyExec()
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a"})
	pool.CreateCommands = []string{"echo hi", "echo bye"}
	pool.HookFailurePolicy = poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE

	reg := metrics.NewRegistry()
	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), reg)
	if err := p.Provision(context.Background(), pool); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	vms := onlyVMsInPool(t, st, "pool-a")
	if len(vms) != 1 {
		t.Fatalf("expected 1 VM record, got %d", len(vms))
	}
	if vms[0].GetPhase() != poolmgrv1alpha1.VMPhase_AVAILABLE {
		t.Fatalf("expected phase AVAILABLE, got %v", vms[0].GetPhase())
	}

	events, err := st.ListEventsSince(context.Background(), "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 2 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_PROVISIONED || events[1].GetType() != poolmgrv1alpha1.EventType_VM_AVAILABLE {
		t.Fatalf("unexpected events: %+v", events)
	}

	body := scrapeMetrics(t, reg)
	if !strings.Contains(body, `poolmgr_vm_provision_duration_seconds_count{pool_name="pool-a",pool_namespace="default"} 1`) {
		t.Fatalf("expected 1 provision duration observation, got:\n%s", body)
	}
	if !strings.Contains(body, `poolmgr_hook_duration_seconds_count{hook="create",pool_name="pool-a",pool_namespace="default"} 1`) {
		t.Fatalf("expected 1 create hook duration observation, got:\n%s", body)
	}
}

func TestProvision_CreatePollTimeout(t *testing.T) {
	vm := &fakeMicroVM{pollsUntilCreated: 1000} // never reaches CREATED within the test's timeout
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a"})
	pool.HookFailurePolicy = poolmgrv1alpha1.HookFailurePolicy_QUARANTINE

	cfg := fastProvisionConfig()
	cfg.CreatePollTimeout = 50 * time.Millisecond
	p := reconciler.NewProvisioner(st, flint, cfg, nil)

	err := p.Provision(context.Background(), pool)
	if !errors.Is(err, reconciler.ErrCreateTimedOut) {
		t.Fatalf("Provision() error = %v, want ErrCreateTimedOut", err)
	}

	vms := onlyVMsInPool(t, st, "pool-a")
	if len(vms) != 1 || vms[0].GetPhase() != poolmgrv1alpha1.VMPhase_QUARANTINED {
		t.Fatalf("expected 1 quarantined VM record, got %+v", vms)
	}
}

func TestProvision_CreateFailedState(t *testing.T) {
	vm := &fakeMicroVM{pollsUntilCreated: 1, failAfterPolls: true}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a"})
	pool.HookFailurePolicy = poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE

	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)
	err := p.Provision(context.Background(), pool)
	if !errors.Is(err, reconciler.ErrCreateFailed) {
		t.Fatalf("Provision() error = %v, want ErrCreateFailed", err)
	}

	if len(onlyVMsInPool(t, st, "pool-a")) != 0 {
		t.Fatalf("expected VM record to be deleted after DELETE_AND_REPLACE")
	}
	if got := vm.deletedUIDs(); len(got) != 1 {
		t.Fatalf("expected DeleteMicroVM to be called once, got %v", got)
	}
}

func TestProvision_GuestAgentNeverReady(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{
		respond: func(*microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			return nil, nil, 0, "", status.Error(codes.Unavailable, "guest agent never ready")
		},
	}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a"})
	pool.HookFailurePolicy = poolmgrv1alpha1.HookFailurePolicy_QUARANTINE

	cfg := fastProvisionConfig()
	cfg.GuestAgentTimeout = 50 * time.Millisecond
	p := reconciler.NewProvisioner(st, flint, cfg, nil)

	err := p.Provision(context.Background(), pool)
	if !errors.Is(err, reconciler.ErrHookFailed) {
		t.Fatalf("Provision() error = %v, want ErrHookFailed", err)
	}

	vms := onlyVMsInPool(t, st, "pool-a")
	if len(vms) != 1 || vms[0].GetPhase() != poolmgrv1alpha1.VMPhase_QUARANTINED {
		t.Fatalf("expected 1 quarantined VM record, got %+v", vms)
	}
}

func TestProvision_CreateCommandNonZeroExit_DeleteAndReplace(t *testing.T) {
	vm := &fakeMicroVM{}
	callCount := 0
	exec := &fakeMicroVMExec{
		respond: func(start *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			callCount++
			if start.GetCmd() == "true" {
				return nil, nil, 0, "", nil // WaitReady probe
			}
			return nil, []byte("boom"), 1, "", nil
		},
	}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a"})
	pool.CreateCommands = []string{"false"}
	pool.HookFailurePolicy = poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE

	reg := metrics.NewRegistry()
	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), reg)
	err := p.Provision(context.Background(), pool)
	if !errors.Is(err, reconciler.ErrHookFailed) {
		t.Fatalf("Provision() error = %v, want ErrHookFailed", err)
	}

	if len(onlyVMsInPool(t, st, "pool-a")) != 0 {
		t.Fatalf("expected VM record to be deleted after DELETE_AND_REPLACE")
	}
	if got := vm.deletedUIDs(); len(got) != 1 {
		t.Fatalf("expected DeleteMicroVM to be called once, got %v", got)
	}

	events, err := st.ListEventsSince(context.Background(), "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	if len(events) != 2 || events[1].GetType() != poolmgrv1alpha1.EventType_VM_HOOK_FAILED {
		t.Fatalf("unexpected events: %+v", events)
	}

	if body := scrapeMetrics(t, reg); !strings.Contains(body, `poolmgr_hook_failures_total{hook="create",pool_name="pool-a",pool_namespace="default"} 1`) {
		t.Fatalf("expected 1 create hook failure recorded, got:\n%s", body)
	}
}

func vmPhasePtr(p poolmgrv1alpha1.VMPhase) *poolmgrv1alpha1.VMPhase { return &p }

func TestProvision_CreateVMStoreFailure_CleansUpOrphanedMicrovm(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := alwaysReadyExec()
	flint := startFakeFlintlock(t, vm, exec)
	st := &failingStore{Store: openTestStore(t), failCreateVM: true}

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a"})

	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)
	err := p.Provision(context.Background(), pool)
	if !errors.Is(err, errInjected) {
		t.Fatalf("Provision() error = %v, want wrapped errInjected", err)
	}

	if got := vm.deletedUIDs(); len(got) != 1 {
		t.Fatalf("expected the orphaned microvm to be deleted, got deleted=%v", got)
	}
	if len(onlyVMsInPool(t, st, "pool-a")) != 0 {
		t.Fatalf("expected no VM record to exist")
	}
}

func TestProvision_UpdatePhaseFailure_AppliesHookFailurePolicy(t *testing.T) {
	tests := []struct {
		name   string
		phase  poolmgrv1alpha1.VMPhase
		policy poolmgrv1alpha1.HookFailurePolicy
	}{
		{"CREATE_HOOK_RUNNING + DELETE_AND_REPLACE", poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING, poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE},
		{"CREATE_HOOK_RUNNING + QUARANTINE", poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING, poolmgrv1alpha1.HookFailurePolicy_QUARANTINE},
		{"AVAILABLE + DELETE_AND_REPLACE", poolmgrv1alpha1.VMPhase_AVAILABLE, poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE},
		{"AVAILABLE + QUARANTINE", poolmgrv1alpha1.VMPhase_AVAILABLE, poolmgrv1alpha1.HookFailurePolicy_QUARANTINE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vm := &fakeMicroVM{}
			exec := alwaysReadyExec()
			flint := startFakeFlintlock(t, vm, exec)
			st := &failingStore{Store: openTestStore(t), failUpdateVMPhase: vmPhasePtr(tt.phase)}

			pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a"})
			pool.HookFailurePolicy = tt.policy

			p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)
			err := p.Provision(context.Background(), pool)
			if !errors.Is(err, errInjected) {
				t.Fatalf("Provision() error = %v, want wrapped errInjected", err)
			}

			vms := onlyVMsInPool(t, st, "pool-a")
			switch tt.policy {
			case poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE:
				if len(vms) != 0 {
					t.Fatalf("expected VM record to be deleted, got %+v", vms)
				}
				if got := vm.deletedUIDs(); len(got) != 1 {
					t.Fatalf("expected DeleteMicroVM to be called once, got %v", got)
				}
			case poolmgrv1alpha1.HookFailurePolicy_QUARANTINE:
				if len(vms) != 1 || vms[0].GetPhase() != poolmgrv1alpha1.VMPhase_QUARANTINED {
					t.Fatalf("expected 1 quarantined VM record, got %+v", vms)
				}
			}
		})
	}
}

// TestProvision_ContextCancelledMidProvision_StillAppliesHookFailurePolicy
// reproduces stopping a pool's Reconciler (e.g. via PoolAdminServer.
// UpdatePool/DeletePool, or poolmgrd shutdown) while a Provision call for
// that pool is in flight: ctx is cancelled after the VMRecord is already
// persisted (PROVISIONING) but before the microvm reaches CREATED.
// ApplyHookFailurePolicy's own cleanup must still land the record in a
// terminal phase (here QUARANTINED) rather than leaving it stranded in
// PROVISIONING forever, which is what happens if that cleanup mistakenly
// runs on the already-cancelled ctx instead of a detached one.
func TestProvision_ContextCancelledMidProvision_StillAppliesHookFailurePolicy(t *testing.T) {
	vm := &fakeMicroVM{pollsUntilCreated: 1000} // never reaches CREATED within this test
	exec := alwaysReadyExec()
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
	pool.HookFailurePolicy = poolmgrv1alpha1.HookFailurePolicy_QUARANTINE

	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Provision(ctx, pool) }()

	// Give CreateVM time to persist the record and at least one poll to
	// happen, then cancel - simulating the owning Reconciler being stopped
	// mid-Provision.
	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Provision() error = nil, want a cancellation-related error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Provision to return after cancel")
	}

	vms := onlyVMsInPool(t, st, "pool-a")
	if len(vms) != 1 {
		t.Fatalf("expected 1 VM record, got %d", len(vms))
	}
	if vms[0].GetPhase() != poolmgrv1alpha1.VMPhase_QUARANTINED {
		t.Fatalf("expected phase QUARANTINED after cancellation, got %v - ApplyHookFailurePolicy's cleanup must not use the already-cancelled ctx", vms[0].GetPhase())
	}
}

func TestProvision_UnknownHost(t *testing.T) {
	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-does-not-exist"})

	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)
	err := p.Provision(context.Background(), pool)
	if !errors.Is(err, flintlockclient.ErrUnknownHost) {
		t.Fatalf("Provision() error = %v, want ErrUnknownHost", err)
	}
}
