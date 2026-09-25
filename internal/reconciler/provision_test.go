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

// TestProvision_AssignsAUniqueIDToEachVM guards against the bug reported in
// https://github.com/liquidmetal-dev/battery/issues/87: the pool's template
// is shared by every VM the reconciler provisions from it, so an empty id
// (the normal case: flintlock-runner and battery's own e2e suite both leave
// it unset, expecting the Pool Manager to assign one) got sent to flintlockd
// as-is, which rejects it ("creating vmid from spec: name is required").
// A fixed non-empty id in the template would only trade that failure for
// every VM in a pool of size > 1 colliding on the same one.
func TestProvision_AssignsAUniqueIDToEachVM(t *testing.T) {
	vm := &fakeMicroVM{pollsUntilCreated: 0}
	exec := alwaysReadyExec()
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a"})
	reg := metrics.NewRegistry()
	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), reg)

	if err := p.Provision(context.Background(), pool); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if err := p.Provision(context.Background(), pool); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	created := vm.createdSpecs()
	if len(created) != 2 {
		t.Fatalf("expected 2 CreateMicroVM calls, got %d", len(created))
	}
	id1, id2 := created[0].GetId(), created[1].GetId()
	if id1 == "" || id2 == "" {
		t.Fatalf("expected every created spec to have a non-empty id, got %q and %q", id1, id2)
	}
	if id1 == id2 {
		t.Fatalf("expected each VM in the pool to get its own id, both got %q", id1)
	}
	if got := created[0].GetNamespace(); got != pool.GetNamespace() {
		t.Fatalf("namespace = %q, want the pool's own %q", got, pool.GetNamespace())
	}
}

// TestProvision_RejectsOldFlintlock guards against
// https://github.com/liquidmetal-dev/battery/issues/94: flintlockd before
// v0.15.2 can put the guest-agent socket at a path too long to dial, so
// Provision must refuse the host before creating anything on it.
func TestProvision_RejectsOldFlintlock(t *testing.T) {
	vm := &fakeMicroVM{serverVersion: "v0.15.1"}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStore(t)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a"})
	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), metrics.NewRegistry())

	err := p.Provision(context.Background(), pool)
	if !errors.Is(err, flintlockclient.ErrUnsupportedVersion) {
		t.Fatalf("Provision() error = %v, want ErrUnsupportedVersion", err)
	}
	if got := len(vm.createdSpecs()); got != 0 {
		t.Fatalf("expected no CreateMicroVM calls, got %d", got)
	}
	if vms := onlyVMsInPool(t, st, "pool-a"); len(vms) != 0 {
		t.Fatalf("expected no VM records, got %d", len(vms))
	}
	if got := countVMsOnHost(t, st, "host-a"); got != 0 {
		t.Fatalf("CountVMsByHost(host-a) = %d after refused host, want 0 (placement must be released)", got)
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
	// Exactly the quarantined row: the placement reservation must have been
	// released even though ctx was cancelled, or it leaks until restart.
	if got := countVMsOnHost(t, st, "host-a"); got != 1 {
		t.Fatalf("CountVMsByHost(host-a) = %d after cancellation, want 1 (reservation must be released on a detached ctx)", got)
	}
}

func countVMsOnHost(t *testing.T, st store.Store, host string) int32 {
	t.Helper()
	got, err := st.CountVMsByHost(context.Background(), host)
	if err != nil {
		t.Fatalf("CountVMsByHost(%q): %v", host, err)
	}
	return got
}

// TestProvision_HostDrainedAfterPick_DoesNotCreateMicroVM is the race from
// the drain review: DrainHost lands after PickHost chose the host but before
// the microvm exists. The placement reservation is checked atomically with
// the drain flag, so the provision must back off without touching flintlock
// and report it as ErrNoEligibleHost (the quiet path in provisionN).
func TestProvision_HostDrainedAfterPick_DoesNotCreateMicroVM(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := &failingStore{Store: openTestStore(t), drainHostBeforeReserve: "host-a"}
	seedHost(t, st, "host-a")

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)

	err := p.Provision(context.Background(), pool)
	if !errors.Is(err, reconciler.ErrNoEligibleHost) {
		t.Fatalf("Provision() error = %v, want ErrNoEligibleHost", err)
	}
	if !errors.Is(err, store.ErrHostDrained) {
		t.Fatalf("Provision() error = %v, want it to also wrap store.ErrHostDrained", err)
	}
	if got := len(vm.createdSpecs()); got != 0 {
		t.Fatalf("expected no CreateMicroVM calls on a host drained mid-provision, got %d", got)
	}
	if vms := onlyVMsInPool(t, st, "pool-a"); len(vms) != 0 {
		t.Fatalf("expected no VM records, got %d", len(vms))
	}
	if got := countVMsOnHost(t, st, "host-a"); got != 0 {
		t.Fatalf("CountVMsByHost(host-a) = %d, want 0", got)
	}
}

// TestProvision_ReservationCountsWhileCreating checks that a host's VM count
// already includes a placement while flintlock is still creating the microvm
// (before any vms row exists), and that the reservation is swapped for the
// row rather than double-counted once the VM is recorded.
func TestProvision_ReservationCountsWhileCreating(t *testing.T) {
	st := openTestStore(t)
	seedHost(t, st, "host-a")

	var duringCreate int32 = -1
	vm := &fakeMicroVM{onCreate: func() {
		// Provision is blocked in CreateMicroVM here, with no store tx open.
		duringCreate = countVMsOnHost(t, st, "host-a")
	}}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)
	if err := p.Provision(context.Background(), pool); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	if duringCreate != 1 {
		t.Fatalf("CountVMsByHost(host-a) during CreateMicroVM = %d, want 1 (the placement reservation)", duringCreate)
	}
	if got := countVMsOnHost(t, st, "host-a"); got != 1 {
		t.Fatalf("CountVMsByHost(host-a) after Provision = %d, want 1 (vms row only; reservation released)", got)
	}
}

// TestProvision_CreateVMFailure_ReleasesReservation: a failure after the
// microvm exists but before its row is written must release the placement
// along with deleting the orphaned microvm.
func TestProvision_CreateVMFailure_ReleasesReservation(t *testing.T) {
	st := &failingStore{Store: openTestStore(t), failCreateVM: true}
	seedHost(t, st, "host-a")

	var duringCreate int32 = -1
	vm := &fakeMicroVM{onCreate: func() { duringCreate = countVMsOnHost(t, st, "host-a") }}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)

	if err := p.Provision(context.Background(), pool); !errors.Is(err, errInjected) {
		t.Fatalf("Provision() error = %v, want wrapped errInjected", err)
	}
	if duringCreate != 1 {
		t.Fatalf("CountVMsByHost(host-a) during CreateMicroVM = %d, want 1", duringCreate)
	}
	if got := countVMsOnHost(t, st, "host-a"); got != 0 {
		t.Fatalf("CountVMsByHost(host-a) after failed Provision = %d, want 0", got)
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
