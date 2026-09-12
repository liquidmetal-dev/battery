package reconciler

import (
	"sync"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// Autoscaler tracks a pool's recent claim history and decides whether an AutoscalingPolicy
// should adjust the pool's target size. One Autoscaler is owned per pool by its Reconciler.
type Autoscaler struct {
	mu           sync.Mutex
	claimTimes   []time.Time
	lastScaledAt time.Time
}

// NewAutoscaler returns an Autoscaler with no claim history and no prior scaling action.
func NewAutoscaler() *Autoscaler {
	return &Autoscaler{}
}

// RecordClaim records a claim at the given time, for later rate calculation.
func (a *Autoscaler) RecordClaim(at time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.claimTimes = append(a.claimTimes, at)
}

// ClaimsPerSec returns the observed claim rate over the trailing window, as of now, pruning
// any recorded claims older than the window as a side effect.
func (a *Autoscaler) ClaimsPerSec(now time.Time, window time.Duration) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pruneLocked(now, window)
	if window <= 0 {
		return 0
	}
	return float64(len(a.claimTimes)) / window.Seconds()
}

// LastScaledAt returns the time of the last scaling action Evaluate decided on, or the zero
// Time if it has never scaled.
func (a *Autoscaler) LastScaledAt() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastScaledAt
}

func (a *Autoscaler) pruneLocked(now time.Time, window time.Duration) {
	cutoff := now.Add(-window)
	kept := a.claimTimes[:0]
	for _, t := range a.claimTimes {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	a.claimTimes = kept
}

// Evaluate decides whether policy's thresholds, given currentSize and the claim rate observed
// as of now, call for a scaling action. It returns the size the pool should have and whether
// that's a change from currentSize. A disabled policy, a cooldown still in effect, or a rate
// inside the dead zone between the two thresholds all result in no change. Scaling up is
// clamped to max_size and scaling down to min_size: a pool already at that bound reports no
// change rather than repeatedly reporting a no-op "scale".
func (a *Autoscaler) Evaluate(policy *poolmgrv1alpha1.AutoscalingPolicy, currentSize int32, now time.Time) (int32, bool) {
	if policy == nil || !policy.GetEnabled() {
		return currentSize, false
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.lastScaledAt.IsZero() && now.Sub(a.lastScaledAt) < policy.GetCooldown().AsDuration() {
		return currentSize, false
	}

	a.pruneLocked(now, policy.GetClaimRateWindow().AsDuration())
	rate := float64(len(a.claimTimes)) / policy.GetClaimRateWindow().AsDuration().Seconds()

	switch {
	case rate > policy.GetScaleUpClaimsPerSec():
		if currentSize >= policy.GetMaxSize() {
			return currentSize, false
		}
		newSize := currentSize + policy.GetScaleStep()
		if newSize > policy.GetMaxSize() {
			newSize = policy.GetMaxSize()
		}
		a.lastScaledAt = now
		return newSize, true
	case rate < policy.GetScaleDownClaimsPerSec():
		if currentSize <= policy.GetMinSize() {
			return currentSize, false
		}
		newSize := currentSize - policy.GetScaleStep()
		if newSize < policy.GetMinSize() {
			newSize = policy.GetMinSize()
		}
		a.lastScaledAt = now
		return newSize, true
	default:
		return currentSize, false
	}
}
