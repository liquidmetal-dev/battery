package reconciler

import (
	"context"
	"errors"
	"log/slog"
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
// pool.RolloutPolicy's batch budget of unavailable VMs.
//
// Replacement is delete-then-provision, and the controller provisions each
// replacement itself rather than leaving it to the pool's Strategy: most
// strategies don't replenish on a deletion (IMMEDIATE_ON_LEASE never does,
// MIN_SIZE_THRESHOLD only once availability falls below min_size), so
// relying on them could delete a warm pool's VMs without ever replacing
// them. For the same reason rollout deletions don't notify the pool's
// Reconciler: REPLACE_ON_DELETE would otherwise provision a second VM for
// each one.
//
// Every replacement the controller owes - from the moment it deletes a
// stale VM until a replacement for it reaches AVAILABLE - is charged against
// the budget. That includes a replacement whose create is still in flight
// (no store row yet) and one whose Provision failed (retried on the next
// Tick), so a slow or failing replacement can never let the rollout delete
// more healthy VMs than the budget allows.
//
// One RolloutController is created per pool (mirroring Reconciler), so Tick
// needs no pool argument and the zero-to-nonzero POOL_ROLLOUT_COMPLETED
// transition can be tracked with a single in-memory field rather than a map
// keyed by pool.
type RolloutController struct {
	pool         *poolmgrv1alpha1.PoolSpec
	store        store.Store
	flint        *flintlockclient.Pool
	provisioner  *Provisioner
	tickInterval time.Duration
	metrics      *metrics.Registry
	log          *slog.Logger

	// replacements tracks the Provision goroutines Tick starts, so Wait
	// (and so Run, on exit) can wait for them.
	replacements sync.WaitGroup

	mu sync.Mutex
	// rollingOut tracks whether the pool had any stale VM as of the end of
	// the previous Tick. POOL_ROLLOUT_COMPLETED fires only on the
	// true-to-false transition, not on every tick while already at zero.
	rollingOut bool
	// started records whether Tick has run before; see adoptDeficit.
	started bool
	// owed is how many replacements this controller has yet to see reach
	// AVAILABLE. provisioning is how many of those have a Provision call
	// running right now; the rest failed and are retried by the next Tick.
	owed         int32
	provisioning int32
}

// NewRolloutController returns a RolloutController for pool, backed by st
// and flint, provisioning replacements with pcfg (see NewProvisioner).
// tickInterval <= 0 uses DefaultRolloutInterval. If m is nil, a fresh
// unshared metrics.Registry is used (see NewProvisioner).
func NewRolloutController(pool *poolmgrv1alpha1.PoolSpec, st store.Store, flint *flintlockclient.Pool, tickInterval time.Duration, pcfg ProvisionConfig, m *metrics.Registry) *RolloutController {
	if tickInterval <= 0 {
		tickInterval = DefaultRolloutInterval
	}
	if m == nil {
		m = metrics.NewRegistry()
	}
	return &RolloutController{
		pool:         pool,
		store:        st,
		flint:        flint,
		provisioner:  NewProvisioner(st, flint, pcfg, m),
		tickInterval: tickInterval,
		metrics:      m,
		log:          slog.Default().With("pool", pool.GetName(), "namespace", pool.GetNamespace()),
	}
}

// Run ticks until ctx is done, then waits for any replacement it started to
// return (ctx cancels them) before returning ctx.Err().
func (c *RolloutController) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.tickInterval)
	defer ticker.Stop()
	defer c.Wait()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.Tick(ctx, time.Now())
		}
	}
}

// Wait blocks until every replacement Provision started by Tick so far has
// returned.
func (c *RolloutController) Wait() {
	c.replacements.Wait()
}

// Tick runs one rollout evaluation as of now. Exported so tests can drive it
// deterministically without waiting on a real ticker. Replacements it
// starts run in the background under ctx; see Wait.
func (c *RolloutController) Tick(ctx context.Context, _ time.Time) {
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

	batch := resolveBatchSize(c.pool.GetRolloutPolicy(), c.pool.GetSize())
	if !c.started {
		c.started = true
		if staleCount > 0 {
			c.adoptDeficit(vms, batch, inFlight)
		}
	}

	// Retry replacements whose Provision failed, before spending any budget
	// on new deletions: they're still owed, and still charged.
	for c.provisioning < c.owed {
		c.startReplacement(ctx)
	}

	budget := batch - inFlight - c.owed
	if budget > 0 && len(candidates) > 0 {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].GetCreatedAt().AsTime().Before(candidates[j].GetCreatedAt().AsTime())
		})
		if int(budget) < len(candidates) {
			candidates = candidates[:budget]
		}
		for _, vm := range candidates {
			// EnsureVMDeletedIfPhase (not EnsureVMDeleted) guards the DELETING
			// transition against candidates being a possibly-stale snapshot:
			// vm may have been concurrently claimed (AVAILABLE -> LEASED) by
			// ClaimAvailableVM between the ListVMsByPool call above and here.
			err := EnsureVMDeletedIfPhase(ctx, c.store, c.flint, vm, poolmgrv1alpha1.VMPhase_AVAILABLE)
			if errors.Is(err, store.ErrPhaseChanged) || errors.Is(err, store.ErrNotFound) {
				// Someone else already changed (or removed) this VM between
				// our snapshot and now - it's no longer a rollout candidate.
				// Leave it completely untouched; a later Tick re-evaluates it
				// from a fresh snapshot if it's still stale and AVAILABLE.
				continue
			}
			if err != nil {
				continue // left DELETING; retried by Sweeper.retryPendingDeletions or a later Tick
			}
			// No notifier: this controller provisions the replacement
			// itself (see the type doc), so the pool's Strategy mustn't.
			FinishVMDeletion(ctx, c.store, c.pool, vm, nil, c.metrics, poolmgrv1alpha1.EventType_VM_DELETED_FOR_ROLLOUT)
			staleCount--
			c.owed++
			c.startReplacement(ctx)
		}
	}

	if staleCount == 0 && c.owed == 0 && c.rollingOut {
		c.rollingOut = false
		EmitEvent(ctx, c.store, c.pool, "", poolmgrv1alpha1.EventType_POOL_ROLLOUT_COMPLETED)
	}
}

// adoptDeficit handles a controller starting mid-rollout, e.g. after a
// poolmgrd restart: the previous controller's owed replacements were only
// in memory, so any of them that never reached AVAILABLE would otherwise
// stop being charged against the budget. It treats the pool's current
// shortfall against its target (see poolDeficit), up to one batch, as owed
// replacements - less stale VMs still DELETING, which Tick already charges
// as inFlight. Caller must hold c.mu.
func (c *RolloutController) adoptDeficit(vms []*poolmgrv1alpha1.VMRecord, batch, inFlight int32) {
	deficit := min(poolDeficit(c.pool, countPhases(vms))-int(inFlight), int(batch))
	if deficit <= 0 {
		return
	}
	c.log.Info("reconciler: rollout resuming with a replacement deficit", "count", deficit)
	c.owed += int32(deficit)
}

// startReplacement provisions one owed replacement in the background. On
// success it's no longer owed; on failure it stays owed (and charged) and
// the next Tick starts it again. Caller must hold c.mu.
func (c *RolloutController) startReplacement(ctx context.Context) {
	c.provisioning++
	c.replacements.Add(1)
	go func() {
		defer c.replacements.Done()
		err := c.provisioner.Provision(ctx, c.pool)
		if err != nil {
			c.log.ErrorContext(ctx, "reconciler: rollout replacement failed; retrying next tick", "error", err)
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		c.provisioning--
		if err == nil {
			c.owed--
		}
	}()
}

// resolveBatchSize resolves how many stale VMs may be deleted in a single
// tick from policy, against a pool of the given size.
//
//   - A nil policy, or one whose oneof is unset, defaults to 1.
//   - An explicit count of 0 is an intentional pause: 0 VMs may be deleted
//     this tick. A negative count is invalid input that shouldn't have
//     reached here (validated pools shouldn't produce one); it falls back to
//     the same default of 1 as an unset policy, rather than pausing.
//   - A percent is resolved against size, rounded up, minimum 1.
//   - The result is clamped to size (a percent > 100, or a count > size,
//     can't usefully unavail more VMs than the pool has), except when size
//     itself is 0 - a pool with no target size at all shouldn't force the
//     minimum-1 floors above down to 0.
func resolveBatchSize(policy *poolmgrv1alpha1.RolloutPolicy, size int32) int32 {
	var n int32
	switch mu := policy.GetMaxUnavailable().(type) {
	case *poolmgrv1alpha1.RolloutPolicy_Count:
		switch {
		case mu.Count == 0:
			return 0
		case mu.Count < 0:
			n = 1
		default:
			n = mu.Count
		}
	case *poolmgrv1alpha1.RolloutPolicy_Percent:
		n = int32(math.Ceil(float64(size) * float64(mu.Percent) / 100))
		if n < 1 {
			n = 1
		}
	default:
		n = 1
	}
	if size > 0 && n > size {
		return size
	}
	return n
}
