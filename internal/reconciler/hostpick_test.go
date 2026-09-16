package reconciler_test

import (
	"context"
	"errors"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
)

func TestPickHost_NoEligibleHost(t *testing.T) {
	st := openTestStore(t)
	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, nil)

	if _, err := reconciler.PickHost(context.Background(), st, pool, nil); !errors.Is(err, reconciler.ErrNoEligibleHost) {
		t.Fatalf("PickHost() error = %v, want ErrNoEligibleHost", err)
	}
}

func TestPickHost_EmptyPoolPicksFirstHost(t *testing.T) {
	st := openTestStore(t)
	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a", "host-b"})

	got, err := reconciler.PickHost(context.Background(), st, pool, nil)
	if err != nil {
		t.Fatalf("PickHost: %v", err)
	}
	if got != "host-a" {
		t.Fatalf("PickHost() = %q, want %q", got, "host-a")
	}
}

func TestPickHost_LeastLoaded(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 5, []string{"host-a", "host-b"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	// host-a: 2 non-terminal VMs, host-b: 1 non-terminal VM + 1 terminal (ignored).
	for _, vm := range []*poolmgrv1alpha1.VMRecord{
		sampleVM("vm-1", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_AVAILABLE),
		sampleVM("vm-2", "pool-a", "host-a", poolmgrv1alpha1.VMPhase_PROVISIONING),
		sampleVM("vm-3", "pool-a", "host-b", poolmgrv1alpha1.VMPhase_LEASED),
		sampleVM("vm-4", "pool-a", "host-b", poolmgrv1alpha1.VMPhase_DELETING),
	} {
		if err := st.CreateVM(ctx, vm); err != nil {
			t.Fatalf("CreateVM(%s): %v", vm.GetUid(), err)
		}
	}

	got, err := reconciler.PickHost(ctx, st, pool, nil)
	if err != nil {
		t.Fatalf("PickHost: %v", err)
	}
	if got != "host-b" {
		t.Fatalf("PickHost() = %q, want %q (fewer non-terminal VMs)", got, "host-b")
	}
}

func TestPickHost_ExcludesDrainedHost(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 5, []string{"host-a", "host-b"})
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	// host-a has no VMs and would normally win on load, but it's drained.
	if err := st.CreateVM(ctx, sampleVM("vm-1", "pool-a", "host-b", poolmgrv1alpha1.VMPhase_LEASED)); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	got, err := reconciler.PickHost(ctx, st, pool, map[string]bool{"host-a": true})
	if err != nil {
		t.Fatalf("PickHost: %v", err)
	}
	if got != "host-b" {
		t.Fatalf("PickHost() = %q, want %q (host-a is drained)", got, "host-b")
	}
}

func TestPickHost_AllHostsDrained(t *testing.T) {
	st := openTestStore(t)
	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 3, []string{"host-a", "host-b"})

	_, err := reconciler.PickHost(context.Background(), st, pool, map[string]bool{"host-a": true, "host-b": true})
	if !errors.Is(err, reconciler.ErrNoEligibleHost) {
		t.Fatalf("PickHost() error = %v, want ErrNoEligibleHost", err)
	}
}
