package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// DefaultTickInterval is how often a Reconciler re-evaluates its pool's
// desired VM count when the caller doesn't specify one.
const DefaultTickInterval = 10 * time.Second

// notifyBuffer is the size of the claimed/deleted notification channels.
// Notifications are coalescing signals, not a queue of individual events:
// a full buffer means a reconcile is already pending, so further
// notifications before it runs are redundant.
const notifyBuffer = 1

// Reconciler runs the control loop for a single pool: on every tick, and on
// every claim/delete notification, it asks the pool's Strategy how many new
// VMs are needed and provisions them.
type Reconciler struct {
	pool         *poolmgrv1alpha1.PoolSpec
	store        store.Store
	strategy     Strategy
	provisioner  *Provisioner
	tickInterval time.Duration

	claimed chan struct{}
	deleted chan struct{}
}

// New returns a Reconciler for pool. tickInterval <= 0 uses
// DefaultTickInterval. If m is nil, a fresh unshared metrics.Registry is
// used (see NewProvisioner).
func New(pool *poolmgrv1alpha1.PoolSpec, st store.Store, flint *flintlockclient.Pool, tickInterval time.Duration, pcfg ProvisionConfig, m *metrics.Registry) (*Reconciler, error) {
	if pool == nil {
		return nil, errors.New("reconciler: pool is required")
	}
	strategy, err := NewStrategy(pool.GetReplenishmentStrategy())
	if err != nil {
		return nil, err
	}
	if tickInterval <= 0 {
		tickInterval = DefaultTickInterval
	}

	return &Reconciler{
		pool:         pool,
		store:        st,
		strategy:     strategy,
		provisioner:  NewProvisioner(st, flint, pcfg, m),
		tickInterval: tickInterval,
		claimed:      make(chan struct{}, notifyBuffer),
		deleted:      make(chan struct{}, notifyBuffer),
	}, nil
}

// NotifyVMClaimed signals that a VM in this pool was just claimed. It never
// blocks: if a notification is already pending, this is a no-op, since
// Run's next pass will observe the same underlying state change either way.
func (r *Reconciler) NotifyVMClaimed() {
	select {
	case r.claimed <- struct{}{}:
	default:
	}
}

// NotifyVMDeleted signals that a VM in this pool was just deleted (expiry,
// release, or a hook failure). Never blocks; see NotifyVMClaimed.
func (r *Reconciler) NotifyVMDeleted() {
	select {
	case r.deleted <- struct{}{}:
	default:
	}
}

// Run drives the control loop until ctx is done, at which point it returns
// ctx.Err(). Each tick and each notification independently computes how
// many VMs to provision and starts that many Provision calls concurrently;
// one failed Provision is logged and does not stop the others or the loop.
func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			counts, err := r.countVMs(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "reconciler: failed to count VMs", "pool", r.pool.GetName(), "error", err)
				continue
			}
			r.provisionN(ctx, r.strategy.DesiredNewVMs(r.pool, counts))
		case <-r.claimed:
			r.provisionN(ctx, r.strategy.OnVMClaimed(r.pool))
		case <-r.deleted:
			r.provisionN(ctx, r.strategy.OnVMDeleted(r.pool))
		}
	}
}

// countVMs summarizes the pool's current VMs into VMCounts.
func (r *Reconciler) countVMs(ctx context.Context) (VMCounts, error) {
	return CountVMs(ctx, r.store, r.pool.GetName(), r.pool.GetNamespace())
}

// CountVMs summarizes a pool's current VMs into VMCounts. Exported so
// callers outside the reconciler's own control loop (e.g. the PoolAdmin API,
// to populate PoolStatus) can get the same phase breakdown without
// duplicating the switch below.
func CountVMs(ctx context.Context, st store.Store, poolName, poolNamespace string) (VMCounts, error) {
	vms, err := st.ListVMsByPool(ctx, poolName, poolNamespace, nil)
	if err != nil {
		return VMCounts{}, fmt.Errorf("reconciler: ListVMsByPool: %w", err)
	}

	var counts VMCounts
	for _, vm := range vms {
		switch vm.GetPhase() {
		case poolmgrv1alpha1.VMPhase_AVAILABLE:
			counts.Available++
		case poolmgrv1alpha1.VMPhase_LEASED, poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING:
			// PRE_LEASE_HOOK_RUNNING is a transient phase before a VM is
			// handed to a consumer: it's already claimed in all but name,
			// so it must count toward Leased or MIN_SIZE_THRESHOLD would
			// see it as neither available nor in-flight and over-provision.
			counts.Leased++
		case poolmgrv1alpha1.VMPhase_PROVISIONING, poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING:
			counts.Provisioning++
		case poolmgrv1alpha1.VMPhase_QUARANTINED:
			counts.Quarantined++
		}
	}
	return counts, nil
}

// provisionN starts n Provision calls concurrently and logs any failures.
// It does not block the caller past all of them completing or ctx being
// done, whichever comes first.
func (r *Reconciler) provisionN(ctx context.Context, n int) {
	if n <= 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := r.provisioner.Provision(ctx, r.pool); err != nil {
				slog.ErrorContext(ctx, "reconciler: provision failed", "pool", r.pool.GetName(), "error", err)
			}
		}()
	}
	wg.Wait()
}
