// Package reconciler implements the per-pool control loop that keeps a
// pool's warm VM count at its target: replenishment strategies decide how
// many new VMs are needed, and the provisioning pipeline turns that into
// AVAILABLE VMRecords by driving flintlock and the pool's create_commands.
package reconciler

import (
	"errors"
	"fmt"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// ErrUnknownStrategy is returned by NewStrategy for a nil spec or an
// unrecognised/unspecified ReplenishmentStrategyType.
var ErrUnknownStrategy = errors.New("reconciler: unknown replenishment strategy")

// ErrMinSizeRequired is returned by NewStrategy for a MIN_SIZE_THRESHOLD
// strategy with no positive min_size. Without it, GetMinSize() defaults to
// 0 and DesiredNewVMs's "Available >= minSize" check is always true, so the
// pool would silently never replenish.
var ErrMinSizeRequired = errors.New("reconciler: MIN_SIZE_THRESHOLD requires a positive min_size")

// VMCounts summarizes a pool's current VM population by phase, as needed by
// a Strategy to decide how many new VMs to provision. Quarantined VMs are
// tracked separately because they never count toward Available.
type VMCounts struct {
	Available    int
	Leased       int
	Provisioning int
	Quarantined  int
}

// Strategy decides how many new VMs a pool's reconciler should start
// provisioning, either on a periodic tick or in response to a claim/delete
// event. Each pool uses exactly one Strategy, selected by its
// ReplenishmentStrategyType; the other two hooks return 0 for a strategy
// that doesn't act on that trigger.
type Strategy interface {
	// DesiredNewVMs is evaluated on every reconcile tick.
	DesiredNewVMs(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int
	// OnVMClaimed is called once per successful claim.
	OnVMClaimed(pool *poolmgrv1alpha1.PoolSpec) int
	// OnVMDeleted is called once per VM deletion (expiry, release, or a
	// DELETE_AND_REPLACE hook failure).
	OnVMDeleted(pool *poolmgrv1alpha1.PoolSpec) int
}

// NewStrategy returns the Strategy implementation for spec.Type.
func NewStrategy(spec *poolmgrv1alpha1.ReplenishmentStrategy) (Strategy, error) {
	if spec == nil {
		return nil, fmt.Errorf("%w: nil strategy", ErrUnknownStrategy)
	}
	switch spec.GetType() {
	case poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE:
		return immediateOnLease{}, nil
	case poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD:
		if spec.MinSize == nil || spec.GetMinSize() <= 0 {
			return nil, ErrMinSizeRequired
		}
		return minSizeThreshold{}, nil
	case poolmgrv1alpha1.ReplenishmentStrategyType_REPLACE_ON_DELETE:
		return replaceOnDelete{}, nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnknownStrategy, spec.GetType())
	}
}

// immediateOnLease starts provisioning one new VM on every successful
// claim; it doesn't act on ticks or deletions.
type immediateOnLease struct{}

func (immediateOnLease) DesiredNewVMs(*poolmgrv1alpha1.PoolSpec, VMCounts) int { return 0 }
func (immediateOnLease) OnVMClaimed(*poolmgrv1alpha1.PoolSpec) int             { return 1 }
func (immediateOnLease) OnVMDeleted(*poolmgrv1alpha1.PoolSpec) int             { return 0 }

// minSizeThreshold tops the pool back up to its target size whenever the
// available count drops below min_size. It doesn't act on claim/delete
// events directly: a claim or deletion changes the available count, which
// the next tick picks up.
type minSizeThreshold struct{}

func (minSizeThreshold) DesiredNewVMs(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int {
	minSize := pool.GetReplenishmentStrategy().GetMinSize()
	if int32(counts.Available) >= minSize {
		return 0
	}

	inFlight := counts.Available + counts.Leased + counts.Provisioning
	need := int(pool.GetSize()) - inFlight
	if need < 0 {
		return 0
	}
	return need
}

func (minSizeThreshold) OnVMClaimed(*poolmgrv1alpha1.PoolSpec) int { return 0 }
func (minSizeThreshold) OnVMDeleted(*poolmgrv1alpha1.PoolSpec) int { return 0 }

// replaceOnDelete starts provisioning exactly one replacement on every VM
// deletion; it doesn't act on ticks or claims.
type replaceOnDelete struct{}

func (replaceOnDelete) DesiredNewVMs(*poolmgrv1alpha1.PoolSpec, VMCounts) int { return 0 }
func (replaceOnDelete) OnVMClaimed(*poolmgrv1alpha1.PoolSpec) int             { return 0 }
func (replaceOnDelete) OnVMDeleted(*poolmgrv1alpha1.PoolSpec) int             { return 1 }
