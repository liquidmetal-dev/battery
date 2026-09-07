package reconciler

import (
	"context"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// DefaultSweepInterval is how often a Sweeper scans for expiring/expired
// leases by default.
const DefaultSweepInterval = 10 * time.Second

// DefaultWarningWindow is how long before a lease's expiry a Sweeper emits
// VM_EXPIRING_SOON by default. It's a process-wide constant rather than a
// per-pool setting: PoolSpec has no warning-window field.
const DefaultWarningWindow = 30 * time.Second

// SweeperNotifier lets a Sweeper nudge whichever component owns a pool's
// Reconciler after it deletes an expired VM (so REPLACE_ON_DELETE pools can
// replenish). Declared separately from api.ReconcilerNotifier (which has
// the same shape) so this package doesn't depend on internal/api; a real
// caller can pass one concrete type satisfying both.
type SweeperNotifier interface {
	NotifyVMDeleted(poolName, poolNamespace string)
}

// noopSweeperNotifier implements SweeperNotifier by doing nothing.
type noopSweeperNotifier struct{}

func (noopSweeperNotifier) NotifyVMDeleted(string, string) {}

// Sweeper periodically scans leases across all pools (store.ListExpiredLeases
// has no per-pool filter, and there's no manager yet to run one sweeper per
// pool against). Each tick it deletes leases/VMs past expiry and emits
// VM_EXPIRING_SOON once per distinct expires_at value for leases about to
// expire within warningWindow.
type Sweeper struct {
	store         store.Store
	flint         *flintlockclient.Pool
	tickInterval  time.Duration
	warningWindow time.Duration
	notifier      SweeperNotifier

	// warned tracks, per lease ID, the expires_at (UnixNano) value we've
	// already emitted VM_EXPIRING_SOON for. A heartbeat changes expires_at,
	// which naturally re-arms the warning with no persistence needed.
	warned map[string]int64
}

// NewSweeper returns a Sweeper backed by st and flint. Zero-valued
// tickInterval/warningWindow are replaced with the Default* constants. If
// notifier is nil, it's a no-op.
func NewSweeper(st store.Store, flint *flintlockclient.Pool, tickInterval, warningWindow time.Duration, notifier SweeperNotifier) *Sweeper {
	if tickInterval <= 0 {
		tickInterval = DefaultSweepInterval
	}
	if warningWindow <= 0 {
		warningWindow = DefaultWarningWindow
	}
	if notifier == nil {
		notifier = noopSweeperNotifier{}
	}
	return &Sweeper{
		store:         st,
		flint:         flint,
		tickInterval:  tickInterval,
		warningWindow: warningWindow,
		notifier:      notifier,
		warned:        make(map[string]int64),
	}
}

// Run ticks until ctx is done.
func (s *Sweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.Tick(ctx, time.Now())
		}
	}
}

// Tick runs one sweep as of now. Exported so tests can drive it
// deterministically without waiting on a real ticker.
func (s *Sweeper) Tick(ctx context.Context, now time.Time) {
	leases, err := s.store.ListExpiredLeases(ctx, now.Add(s.warningWindow))
	if err != nil {
		return // best-effort; try again next tick
	}

	seen := make(map[string]struct{}, len(leases))
	for _, l := range leases {
		seen[l.GetLeaseId()] = struct{}{}

		expiresAt := l.GetExpiresAt().AsTime()
		if !expiresAt.After(now) {
			s.expire(ctx, l)
			delete(s.warned, l.GetLeaseId())
			continue
		}

		key := l.GetLeaseId()
		if last, ok := s.warned[key]; !ok || last != expiresAt.UnixNano() {
			s.warnExpiringSoon(ctx, l)
			s.warned[key] = expiresAt.UnixNano()
		}
	}

	// Evict warned entries for leases no longer returned by the query
	// (released, or already expired+deleted) so the map doesn't grow
	// unbounded.
	for id := range s.warned {
		if _, ok := seen[id]; !ok {
			delete(s.warned, id)
		}
	}
}

func (s *Sweeper) warnExpiringSoon(ctx context.Context, l *poolmgrv1alpha1.LeaseRecord) {
	pool, err := s.store.GetPool(ctx, l.GetPoolName(), l.GetPoolNamespace())
	if err != nil {
		return
	}
	EmitEvent(ctx, s.store, pool, l.GetVmUid(), poolmgrv1alpha1.EventType_VM_EXPIRING_SOON)
}

// expire deletes the lease's VM (via flintlock and the store) and the lease
// row, and emits VM_DELETED_DUE_TO_EXPIRY. If the VM is already gone or has
// moved out of LEASED (a race with a concurrent ReleaseVM), it just drops
// the stale lease row without touching flintlock or emitting an event.
func (s *Sweeper) expire(ctx context.Context, l *poolmgrv1alpha1.LeaseRecord) {
	vm, err := s.store.GetVM(ctx, l.GetVmUid())
	if err != nil {
		_ = s.store.DeleteLease(ctx, l.GetLeaseId())
		return
	}
	if vm.GetPhase() != poolmgrv1alpha1.VMPhase_LEASED {
		_ = s.store.DeleteLease(ctx, l.GetLeaseId())
		return
	}

	pool, err := s.store.GetPool(ctx, l.GetPoolName(), l.GetPoolNamespace())
	if err != nil {
		return // try again next tick
	}

	if client, cerr := s.flint.Client(vm.GetFlintlockHost()); cerr == nil {
		_, _ = client.DeleteMicroVM(ctx, &microvmv1alpha1.DeleteMicroVMRequest{Uid: vm.GetUid()})
	}
	if err := s.store.DeleteVM(ctx, vm.GetUid()); err != nil {
		return // leave the lease in place, retry next tick
	}

	EmitEvent(ctx, s.store, pool, vm.GetUid(), poolmgrv1alpha1.EventType_VM_DELETED_DUE_TO_EXPIRY)
	_ = s.store.DeleteLease(ctx, l.GetLeaseId())
	s.notifier.NotifyVMDeleted(l.GetPoolName(), l.GetPoolNamespace())
}
