package reconciler

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// DefaultRolloutInterval is how often a RolloutController re-evaluates its
// pool for stale VMs to replace, when the caller doesn't specify one.
const DefaultRolloutInterval = 10 * time.Second

// RolloutController periodically replaces AVAILABLE VMs whose template_hash
// no longer matches its pool's current TemplateHash (set by CreatePool/
// UpdatePool - see internal/api/pooladmin.go's computeTemplateHash), up to
// pool.RolloutPolicy's batch budget per tick. Replacement VMs are
// provisioned for free by the pool's existing Reconciler: EnsureVMDeleted
// removes the stale VM's store row, and the Reconciler's own
// NotifyVMDeleted/deleted-channel path (via notifier) triggers
// Strategy.OnVMDeleted to provision a replacement.
//
// One RolloutController is created per pool (mirroring Reconciler), so Tick
// needs no pool argument and the zero-to-nonzero POOL_ROLLOUT_COMPLETED
// transition can be tracked with a single in-memory field rather than a map
// keyed by pool.
type RolloutController struct {
	pool         *poolmgrv1alpha1.PoolSpec
	store        store.Store
	flint        *flintlockclient.Pool
	tickInterval time.Duration
	notifier     DeletionNotifier
	metrics      *metrics.Registry

	mu sync.Mutex
	// rollingOut tracks whether the pool had any stale VM as of the end of
	// the previous Tick. POOL_ROLLOUT_COMPLETED fires only on the
	// true-to-false transition, not on every tick while already at zero.
	rollingOut bool
}

// NewRolloutController returns a RolloutController for pool, backed by st
// and flint. tickInterval <= 0 uses DefaultRolloutInterval. If notifier is
// nil, it's a no-op (see NewSweeper). If m is nil, a fresh unshared
// metrics.Registry is used (see NewProvisioner).
func NewRolloutController(pool *poolmgrv1alpha1.PoolSpec, st store.Store, flint *flintlockclient.Pool, tickInterval time.Duration, notifier DeletionNotifier, m *metrics.Registry) *RolloutController {
	if tickInterval <= 0 {
		tickInterval = DefaultRolloutInterval
	}
	if notifier == nil {
		notifier = noopSweeperNotifier{}
	}
	if m == nil {
		m = metrics.NewRegistry()
	}
	return &RolloutController{
		pool:         pool,
		store:        st,
		flint:        flint,
		tickInterval: tickInterval,
		notifier:     notifier,
		metrics:      m,
	}
}

// Run ticks until ctx is done.
func (c *RolloutController) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.Tick(ctx, time.Now())
		}
	}
}

// Tick runs one rollout evaluation as of now. Exported so tests can drive it
// deterministically without waiting on a real ticker.
func (c *RolloutController) Tick(ctx context.Context, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentHash := c.pool.GetTemplateHash()

	vms, err := c.store.ListVMsByPool(ctx, c.pool.GetName(), c.pool.GetNamespace(), nil)
	if err != nil {
		return // best-effort; try again next tick
	}

	var (
		staleCount int32
		candidates []*poolmgrv1alpha1.VMRecord
		inFlight   int32
	)
	for _, vm := range vms {
		if vm.GetTemplateHash() == currentHash {
			continue
		}
		staleCount++
		switch vm.GetPhase() {
		case poolmgrv1alpha1.VMPhase_AVAILABLE:
			candidates = append(candidates, vm)
		case poolmgrv1alpha1.VMPhase_DELETING:
			inFlight++
		}
	}

	if staleCount > 0 {
		c.rollingOut = true
	}

	budget := resolveBatchSize(c.pool.GetRolloutPolicy(), c.pool.GetSize()) - inFlight
	if budget > 0 && len(candidates) > 0 {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].GetCreatedAt().AsTime().Before(candidates[j].GetCreatedAt().AsTime())
		})
		if int(budget) < len(candidates) {
			candidates = candidates[:budget]
		}
		for _, vm := range candidates {
			if err := EnsureVMDeleted(ctx, c.store, c.flint, vm); err != nil {
				continue // left DELETING; retried by Sweeper.retryPendingDeletions or a later Tick
			}
			FinishVMDeletion(ctx, c.store, c.pool, vm, c.notifier, c.metrics, poolmgrv1alpha1.EventType_VM_DELETED_FOR_ROLLOUT)
			staleCount--
		}
	}

	if staleCount == 0 && c.rollingOut {
		c.rollingOut = false
		EmitEvent(ctx, c.store, c.pool, "", poolmgrv1alpha1.EventType_POOL_ROLLOUT_COMPLETED)
	}
}

// resolveBatchSize resolves how many stale VMs may be deleted in a single
// tick from policy, against a pool of the given size. A nil policy, or one
// whose oneof is unset, defaults to 1. A percent is resolved against size,
// rounded up, minimum 1.
func resolveBatchSize(policy *poolmgrv1alpha1.RolloutPolicy, size int32) int32 {
	switch mu := policy.GetMaxUnavailable().(type) {
	case *poolmgrv1alpha1.RolloutPolicy_Count:
		if mu.Count < 1 {
			return 1
		}
		return mu.Count
	case *poolmgrv1alpha1.RolloutPolicy_Percent:
		n := int32(math.Ceil(float64(size) * float64(mu.Percent) / 100))
		if n < 1 {
			return 1
		}
		return n
	default:
		return 1
	}
}
