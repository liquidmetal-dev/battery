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
// provisioning: when the reconciler starts and on every periodic tick, or in
// response to a claim/delete event. Each pool uses exactly one Strategy,
// selected by its ReplenishmentStrategyType. Every hook is level-triggered:
// it returns the pool's current shortfall given counts, never a fixed
// per-event amount, so a provision that failed or an event that was
// coalesced away is made up by whichever hook runs next. A hook returns 0
// for a trigger its strategy doesn't act on.
type Strategy interface {
	// DesiredNewVMs is evaluated when a reconciler starts and on every
	// reconcile tick.
	DesiredNewVMs(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int
	// OnVMClaimed is evaluated after a successful claim.
	OnVMClaimed(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int
	// OnVMDeleted is evaluated after a VM deletion (expiry, release, or a
	// DELETE_AND_REPLACE hook failure).
	OnVMDeleted(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int
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

// immediateOnLease keeps size warm VMs: leased VMs don't count, since each
// claim adds a VM on top of the warm set rather than drawing it down. A
// claim triggers the top-up immediately; the tick repeats it, so a fresh
// pool (nothing to claim yet) fills and a failed provision is retried.
type immediateOnLease struct{}

func (immediateOnLease) shortfall(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int {
	return max(0, int(pool.GetSize())-(counts.Available+counts.Provisioning))
}

func (s immediateOnLease) DesiredNewVMs(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int {
	return s.shortfall(pool, counts)
}

func (s immediateOnLease) OnVMClaimed(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int {
	return s.shortfall(pool, counts)
}

func (immediateOnLease) OnVMDeleted(*poolmgrv1alpha1.PoolSpec, VMCounts) int { return 0 }

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

func (minSizeThreshold) OnVMClaimed(*poolmgrv1alpha1.PoolSpec, VMCounts) int { return 0 }
func (minSizeThreshold) OnVMDeleted(*poolmgrv1alpha1.PoolSpec, VMCounts) int { return 0 }

// replaceOnDelete keeps size VMs in total (leased included), so a claim
// doesn't replenish but a deletion does. A deletion triggers the top-up
// immediately; the tick repeats it, so a fresh pool (nothing to delete yet)
// fills and a failed provision is retried.
type replaceOnDelete struct{}

func (replaceOnDelete) shortfall(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int {
	return max(0, int(pool.GetSize())-(counts.Available+counts.Leased+counts.Provisioning))
}

func (s replaceOnDelete) DesiredNewVMs(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int {
	return s.shortfall(pool, counts)
}

func (replaceOnDelete) OnVMClaimed(*poolmgrv1alpha1.PoolSpec, VMCounts) int { return 0 }

func (s replaceOnDelete) OnVMDeleted(pool *poolmgrv1alpha1.PoolSpec, counts VMCounts) int {
	return s.shortfall(pool, counts)
}
