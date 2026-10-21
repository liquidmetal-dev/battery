package reconciler_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// poolEvents returns the types of every event for pool-a in st.
func poolEvents(t *testing.T, st store.Store) []poolmgrv1alpha1.EventType {
	t.Helper()
	events, err := st.ListEventsSince(context.Background(), "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince: %v", err)
	}
	return eventTypes(t, events)
}

// TestSweeper_DrainDeletesLateProvision covers a provision that reserved
// its placement before the drain: DrainHost commits while flintlock creates
// the microvm, finds no row to mark, and the provision finishes as
// AVAILABLE. The Sweeper's next tick must delete it.
func TestSweeper_DrainDeletesLateProvision(t *testing.T) {
	ctx := context.Background()
	st := openTestStoreWithHost(t, "host-a")

	vm := &fakeMicroVM{onCreate: func() {
		if _, _, err := st.DrainHost(context.Background(), "host-a", "scale-down"); err != nil {
			t.Errorf("DrainHost: %v", err)
		}
	}}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)
	if err := p.Provision(ctx, pool); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	vms := onlyVMsInPool(t, st, "pool-a")
	if len(vms) != 1 || vms[0].GetPhase() != poolmgrv1alpha1.VMPhase_AVAILABLE {
		t.Fatalf("expected one AVAILABLE VM on the drained host before the sweep, got %+v", vms)
	}
	uid := vms[0].GetUid()

	notifier := &spyNotifier{}
	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, notifier, nil)
	sweeper.Tick(ctx, time.Now())

	if got := onlyVMsInPool(t, st, "pool-a"); len(got) != 0 {
		t.Fatalf("expected the late VM deleted, got %+v", got)
	}
	if got := vm.deletedUIDs(); !slices.Equal(got, []string{uid}) {
		t.Fatalf("DeleteMicroVM calls = %v, want [%s]", got, uid)
	}
	if got := poolEvents(t, st); !slices.Contains(got, poolmgrv1alpha1.EventType_VM_DELETED_ON_HOST_DRAIN) {
		t.Fatalf("events = %v, want VM_DELETED_ON_HOST_DRAIN", got)
	}
	if !slices.Equal(notifier.deleted, []string{"default/pool-a"}) {
		t.Fatalf("NotifyVMDeleted calls = %v, want [default/pool-a]", notifier.deleted)
	}
}

// TestProvision_DrainedMidProvision covers a drain that marks a VM DELETING
// while its create hook runs. The provision must not bring it back as
// AVAILABLE, and must not report a hook failure that did not happen.
func TestProvision_DrainedMidProvision(t *testing.T) {
	ctx := context.Background()
	st := openTestStoreWithHost(t, "host-a")

	vm := &fakeMicroVM{}
	exec := &fakeMicroVMExec{
		respond: func(start *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			if start.GetCmd() == "drain-now" {
				if _, _, err := st.DrainHost(context.Background(), "host-a", ""); err != nil {
					t.Errorf("DrainHost: %v", err)
				}
			}
			return nil, nil, 0, "", nil
		},
	}
	flint := startFakeFlintlock(t, vm, exec)

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
	pool.CreateCommands = []string{"drain-now"}
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	reg := metrics.NewRegistry()
	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), reg)
	err := p.Provision(ctx, pool)
	if !errors.Is(err, store.ErrVMDeleting) {
		t.Fatalf("Provision() error = %v, want ErrVMDeleting", err)
	}

	vms := onlyVMsInPool(t, st, "pool-a")
	if len(vms) != 1 || vms[0].GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Fatalf("expected the VM left DELETING for the drain, got %+v", vms)
	}
	if got := poolEvents(t, st); slices.Contains(got, poolmgrv1alpha1.EventType_VM_HOOK_FAILED) || slices.Contains(got, poolmgrv1alpha1.EventType_VM_AVAILABLE) {
		t.Fatalf("events = %v, want neither VM_HOOK_FAILED nor VM_AVAILABLE", got)
	}
	if body := scrapeMetrics(t, reg); strings.Contains(body, "poolmgr_hook_failures_total{") {
		t.Fatalf("expected no hook failure recorded, got:\n%s", body)
	}

	// The Sweeper finishes the delete. The VM is unleased on a draining
	// host, so it is not reported as an expiry.
	notifier := &spyNotifier{}
	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, notifier, nil)
	sweeper.Tick(ctx, time.Now())

	if got := onlyVMsInPool(t, st, "pool-a"); len(got) != 0 {
		t.Fatalf("expected the VM deleted, got %+v", got)
	}
	if got := poolEvents(t, st); slices.Contains(got, poolmgrv1alpha1.EventType_VM_DELETED_DUE_TO_EXPIRY) {
		t.Fatalf("events = %v, want no VM_DELETED_DUE_TO_EXPIRY", got)
	}
	if !slices.Equal(notifier.deleted, []string{"default/pool-a"}) {
		t.Fatalf("NotifyVMDeleted calls = %v, want [default/pool-a]", notifier.deleted)
	}
}

// TestSweeper_DrainRetriesFailedDelete checks that a drained VM whose
// delete fails is left DELETING, retried on the next tick, and reported
// once, as a drain.
func TestSweeper_DrainRetriesFailedDelete(t *testing.T) {
	ctx := context.Background()
	st := openTestStoreWithHost(t, "host-a")
	vm := &fakeMicroVM{failDeletesRemaining: 1}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if _, _, err := st.DrainHost(ctx, "host-a", ""); err != nil {
		t.Fatalf("DrainHost: %v", err)
	}
	// A VM quarantined after the drain, such as by a failed pre-lease hook:
	// the Sweeper's pass must find it on its own.
	if err := st.CreateVM(ctx, sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_QUARANTINED)); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	notifier := &spyNotifier{}
	sweeper := reconciler.NewSweeper(st, flint, time.Second, 30*time.Second, notifier, nil)

	sweeper.Tick(ctx, time.Now())
	got, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM after failed delete: %v", err)
	}
	if got.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Fatalf("phase after failed delete = %v, want DELETING", got.GetPhase())
	}

	sweeper.Tick(ctx, time.Now())
	if _, err := st.GetVM(ctx, "vm-1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetVM after retry error = %v, want ErrNotFound", err)
	}

	if got := poolEvents(t, st); !slices.Equal(got, []poolmgrv1alpha1.EventType{poolmgrv1alpha1.EventType_VM_DELETED_ON_HOST_DRAIN}) {
		t.Fatalf("events = %v, want one VM_DELETED_ON_HOST_DRAIN", got)
	}
	if !slices.Equal(notifier.deleted, []string{"default/pool-a"}) {
		t.Fatalf("NotifyVMDeleted calls = %v, want [default/pool-a]", notifier.deleted)
	}
}

// TestSweeper_DrainLeavesLeasedVMs checks that a leased VM on a draining
// host is left alone until its lease ends, and then ends as an expiry.
func TestSweeper_DrainLeavesLeasedVMs(t *testing.T) {
	ctx := context.Background()
	st := openTestStoreWithHost(t, "host-a")
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	leased := sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_LEASED)
	leaseID := "lease-1"
	leased.LeaseId = &leaseID
	if err := st.CreateVM(ctx, leased); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	now := time.Now()
	if err := st.CreateLease(ctx, sampleLease(leaseID, "vm-1", "pool-a", now.Add(time.Hour))); err != nil {
		t.Fatalf("CreateLease: %v", err)
	}
	if _, marked, err := st.DrainHost(ctx, "host-a", ""); err != nil || len(marked) != 0 {
		t.Fatalf("DrainHost() = %v, %v, want nothing marked", marked, err)
	}

	sweeper := reconciler.NewSweeper(st, flint, time.Second, time.Second, nil, nil)
	sweeper.Tick(ctx, now)

	got, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM: %v", err)
	}
	if got.GetPhase() != poolmgrv1alpha1.VMPhase_LEASED {
		t.Fatalf("leased VM phase = %v, want LEASED", got.GetPhase())
	}
	if len(vm.deletedUIDs()) != 0 {
		t.Fatalf("expected no DeleteMicroVM calls, got %v", vm.deletedUIDs())
	}

	// Once the lease expires, the VM goes as any expired VM does.
	sweeper.Tick(ctx, now.Add(2*time.Hour))
	if _, err := st.GetVM(ctx, "vm-1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetVM after expiry error = %v, want ErrNotFound", err)
	}
	if got := poolEvents(t, st); !slices.Equal(got, []poolmgrv1alpha1.EventType{poolmgrv1alpha1.EventType_VM_DELETED_DUE_TO_EXPIRY}) {
		t.Fatalf("events = %v, want one VM_DELETED_DUE_TO_EXPIRY", got)
	}
	count, err := st.CountVMsByHost(ctx, "host-a")
	if err != nil {
		t.Fatalf("CountVMsByHost: %v", err)
	}
	if count != 0 {
		t.Fatalf("vm_count after the last lease ended = %d, want 0", count)
	}
}

// TestSweeper_UncordonEndsDrain checks that the Sweeper leaves a host's VMs
// alone once UncordonHost has ended its drain.
func TestSweeper_UncordonEndsDrain(t *testing.T) {
	ctx := context.Background()
	st := openTestStoreWithHost(t, "host-a")
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if _, _, err := st.DrainHost(ctx, "host-a", ""); err != nil {
		t.Fatalf("DrainHost: %v", err)
	}
	if _, err := st.SetHostCordoned(ctx, "host-a", false, ""); err != nil {
		t.Fatalf("SetHostCordoned(false): %v", err)
	}
	if err := st.CreateVM(ctx, sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	sweeper := reconciler.NewSweeper(st, flint, time.Second, time.Second, nil, nil)
	sweeper.Tick(ctx, time.Now())

	got, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM: %v", err)
	}
	if got.GetPhase() != poolmgrv1alpha1.VMPhase_AVAILABLE {
		t.Fatalf("phase after uncordon = %v, want AVAILABLE", got.GetPhase())
	}
}
