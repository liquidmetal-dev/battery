package reconciler_test

import (
	"errors"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
)

func poolWithStrategy(strategyType poolmgrv1alpha1.ReplenishmentStrategyType, size, minSize int32) *poolmgrv1alpha1.PoolSpec {
	return &poolmgrv1alpha1.PoolSpec{
		Name:      "pool-a",
		Namespace: "default",
		Size:      size,
		ReplenishmentStrategy: &poolmgrv1alpha1.ReplenishmentStrategy{
			Type:    strategyType,
			MinSize: &minSize,
		},
	}
}

func TestNewStrategy_Unknown(t *testing.T) {
	if _, err := reconciler.NewStrategy(nil); !errors.Is(err, reconciler.ErrUnknownStrategy) {
		t.Fatalf("NewStrategy(nil) error = %v, want ErrUnknownStrategy", err)
	}

	unspecified := &poolmgrv1alpha1.ReplenishmentStrategy{}
	if _, err := reconciler.NewStrategy(unspecified); !errors.Is(err, reconciler.ErrUnknownStrategy) {
		t.Fatalf("NewStrategy(unspecified) error = %v, want ErrUnknownStrategy", err)
	}
}

func TestNewStrategy_MinSizeRequired(t *testing.T) {
	zero := int32(0)
	negative := int32(-1)
	tests := []struct {
		name    string
		minSize *int32
	}{
		{"nil min_size", nil},
		{"zero min_size", &zero},
		{"negative min_size", &negative},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &poolmgrv1alpha1.ReplenishmentStrategy{
				Type:    poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD,
				MinSize: tt.minSize,
			}
			if _, err := reconciler.NewStrategy(spec); !errors.Is(err, reconciler.ErrMinSizeRequired) {
				t.Fatalf("NewStrategy() error = %v, want ErrMinSizeRequired", err)
			}
		})
	}
}

func TestImmediateOnLease(t *testing.T) {
	pool := poolWithStrategy(poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE, 5, 2)
	s, err := reconciler.NewStrategy(pool.GetReplenishmentStrategy())
	if err != nil {
		t.Fatalf("NewStrategy: %v", err)
	}

	if got := s.DesiredNewVMs(pool, reconciler.VMCounts{}); got != 0 {
		t.Errorf("DesiredNewVMs() = %d, want 0", got)
	}
	if got := s.OnVMClaimed(pool); got != 1 {
		t.Errorf("OnVMClaimed() = %d, want 1", got)
	}
	if got := s.OnVMDeleted(pool); got != 0 {
		t.Errorf("OnVMDeleted() = %d, want 0", got)
	}
}

func TestReplaceOnDelete(t *testing.T) {
	pool := poolWithStrategy(poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE, 5, 2)
	s, err := reconciler.NewStrategy(pool.GetReplenishmentStrategy())
	if err != nil {
		t.Fatalf("NewStrategy: %v", err)
	}

	if got := s.DesiredNewVMs(pool, reconciler.VMCounts{}); got != 0 {
		t.Errorf("DesiredNewVMs() = %d, want 0", got)
	}
	if got := s.OnVMClaimed(pool); got != 0 {
		t.Errorf("OnVMClaimed() = %d, want 0", got)
	}
	if got := s.OnVMDeleted(pool); got != 1 {
		t.Errorf("OnVMDeleted() = %d, want 1", got)
	}
}

func TestMinSizeThreshold_DesiredNewVMs(t *testing.T) {
	tests := []struct {
		name    string
		size    int32
		minSize int32
		counts  reconciler.VMCounts
		want    int
	}{
		{
			name:    "available below min_size tops up to size",
			size:    5,
			minSize: 2,
			counts:  reconciler.VMCounts{Available: 1},
			want:    4,
		},
		{
			name:    "available at min_size does nothing",
			size:    5,
			minSize: 2,
			counts:  reconciler.VMCounts{Available: 2},
			want:    0,
		},
		{
			name:    "available above min_size does nothing",
			size:    5,
			minSize: 2,
			counts:  reconciler.VMCounts{Available: 3},
			want:    0,
		},
		{
			name:    "leased and provisioning count toward in-flight total",
			size:    5,
			minSize: 4,
			counts:  reconciler.VMCounts{Available: 1, Leased: 2, Provisioning: 1},
			want:    1,
		},
		{
			name:    "already at or above target size provisions nothing",
			size:    3,
			minSize: 4,
			counts:  reconciler.VMCounts{Available: 1, Leased: 2, Provisioning: 1},
			want:    0,
		},
		{
			name:    "quarantined VMs don't count as available",
			size:    5,
			minSize: 2,
			counts:  reconciler.VMCounts{Available: 1, Quarantined: 3},
			want:    4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := poolWithStrategy(poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, tt.size, tt.minSize)
			s, err := reconciler.NewStrategy(pool.GetReplenishmentStrategy())
			if err != nil {
				t.Fatalf("NewStrategy: %v", err)
			}
			if got := s.DesiredNewVMs(pool, tt.counts); got != tt.want {
				t.Errorf("DesiredNewVMs() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestMinSizeThreshold_EventsAreNoop(t *testing.T) {
	pool := poolWithStrategy(poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 5, 2)
	s, err := reconciler.NewStrategy(pool.GetReplenishmentStrategy())
	if err != nil {
		t.Fatalf("NewStrategy: %v", err)
	}
	if got := s.OnVMClaimed(pool); got != 0 {
		t.Errorf("OnVMClaimed() = %d, want 0", got)
	}
	if got := s.OnVMDeleted(pool); got != 0 {
		t.Errorf("OnVMDeleted() = %d, want 0", got)
	}
}
