package reconciler_test

import (
	"path/filepath"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/store"
)

// openTestStore returns a fresh SQLite-backed Store in a temp dir, closed
// automatically at the end of the test. Mirrors internal/store's own
// openTestStore helper, which is unexported and so can't be reused directly
// from this external test package.
func openTestStore(t *testing.T) store.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "poolmgr.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return s
}

// samplePool returns a minimal, valid PoolSpec using the given strategy and
// hosts, suitable as-is for CreatePool.
func samplePool(name string, strategyType poolmgrv1alpha1.ReplenishmentStrategyType, size int32, hosts []string) *poolmgrv1alpha1.PoolSpec {
	return &poolmgrv1alpha1.PoolSpec{
		Name:            name,
		Namespace:       "default",
		Size:            size,
		FlintlockHosts:  hosts,
		MicrovmTemplate: &flintlocktypes.MicroVMSpec{Vcpu: 1},
		ReplenishmentStrategy: &poolmgrv1alpha1.ReplenishmentStrategy{
			Type: strategyType,
		},
		HookFailurePolicy:        poolmgrv1alpha1.HookFailurePolicy_QUARANTINE,
		HeartbeatInterval:        durationpb.New(30_000_000_000),
		HeartbeatExpiryThreshold: durationpb.New(90_000_000_000),
	}
}

// sampleVM returns a minimal VMRecord for pool on host in phase.
func sampleVM(uid, poolName, host string, phase poolmgrv1alpha1.VMPhase) *poolmgrv1alpha1.VMRecord {
	now := timestamppb.Now()
	return &poolmgrv1alpha1.VMRecord{
		Uid:           uid,
		PoolName:      poolName,
		PoolNamespace: "default",
		FlintlockHost: host,
		Phase:         phase,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}
