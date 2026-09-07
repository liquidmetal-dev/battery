package reconciler

import (
	"context"
	"sync"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"

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

// SweeperNotifier is DeletionNotifier under the name a Sweeper caller
// reaches for; the two are interchangeable (declared separately from
// api.ReconcilerNotifier, which has the same shape, so this package doesn't
// depend on internal/api - a real caller can pass one concrete type
// satisfying both).
type SweeperNotifier = DeletionNotifier

// noopSweeperNotifier implements SweeperNotifier by doing nothing.
type noopSweeperNotifier struct{}

func (noopSweeperNotifier) NotifyVMDeleted(string, string) {}

// Sweeper periodically scans leases across all pools (store.ListExpiredLeases
// has no per-pool filter, and there's no manager yet to run one sweeper per
// pool against). Each tick it: retries any VM deletions left pending by a
// previous failure (flintlock unreachable, etc.), deletes leases/VMs past
// expiry, and emits VM_EXPIRING_SOON once per distinct expires_at value for
// leases about to expire within warningWindow.
//
// Tick (and therefore Run) is safe to call concurrently: mu serializes each
// sweep, including the callers that also invoke EnsureVMDeleted/
// FinishVMDeletion directly (ReleaseVM) against a concurrently-running
// Sweeper via the store's own guarded operations - Tick itself only needs
// the mutex to protect the in-memory warned map.
type Sweeper struct {
	store         store.Store
	flint         *flintlockclient.Pool
	tickInterval  time.Duration
	warningWindow time.Duration
	notifier      SweeperNotifier

	mu sync.Mutex
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
// deterministically without waiting on a real ticker; safe to call
// concurrently (see Sweeper's doc comment).
func (s *Sweeper) Tick(ctx context.Context, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retryPendingDeletions(ctx)

	leases, err := s.store.ListExpiredLeases(ctx, now.Add(s.warningWindow))
	if err != nil {
		return // best-effort; try again next tick
	}

	seen := make(map[string]struct{}, len(leases))
	for _, l := range leases {
		seen[l.GetLeaseId()] = struct{}{}

		expiresAt := l.GetExpiresAt().AsTime()
		if !expiresAt.After(now) {
			s.beginExpiry(ctx, l, now)
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

// retryPendingDeletions finishes any VM deletion left incomplete by a
// previous EnsureVMDeleted failure (from a prior tick's beginExpiry, or from
// a ReleaseVM call whose flintlock request failed).
func (s *Sweeper) retryPendingDeletions(ctx context.Context) {
	vms, err := s.store.ListVMsByPhase(ctx, poolmgrv1alpha1.VMPhase_DELETING)
	if err != nil {
		return // best-effort; try again next tick
	}
	for _, vm := range vms {
		if err := EnsureVMDeleted(ctx, s.store, s.flint, vm); err != nil {
			continue // still pending; retry next tick
		}
		pool, err := s.store.GetPool(ctx, vm.GetPoolName(), vm.GetPoolNamespace())
		if err != nil {
			continue
		}
		FinishVMDeletion(ctx, s.store, pool, vm, s.notifier)
	}
}

func (s *Sweeper) warnExpiringSoon(ctx context.Context, l *poolmgrv1alpha1.LeaseRecord) {
	pool, err := s.store.GetPool(ctx, l.GetPoolName(), l.GetPoolNamespace())
	if err != nil {
		return
	}
	EmitEvent(ctx, s.store, pool, l.GetVmUid(), poolmgrv1alpha1.EventType_VM_EXPIRING_SOON)
}

// beginExpiry atomically claims l for deletion (re-validating its expiry
// against now, so a Heartbeat that renewed l between the ListExpiredLeases
// snapshot and this call is never lost - see store.DeleteLeaseIfExpired)
// and, if successful, deletes its VM. If the claim fails (already gone, or
// renewed) there's nothing to do. If the VM is gone or has moved out of
// LEASED (a race with a concurrent ReleaseVM that finished first), the
// lease is already correctly deleted and there's nothing more to do. If
// EnsureVMDeleted fails, the VM is left DELETING for retryPendingDeletions
// to finish on a later tick.
func (s *Sweeper) beginExpiry(ctx context.Context, l *poolmgrv1alpha1.LeaseRecord, now time.Time) {
	if _, err := s.store.DeleteLeaseIfExpired(ctx, l.GetLeaseId(), now); err != nil {
		return // ErrNotFound, ErrLeaseNotExpired, or a transient store error: safe to skip/retry later
	}

	vm, err := s.store.GetVM(ctx, l.GetVmUid())
	if err != nil {
		// VM already gone, or (rarely) a transient store error reading it -
		// the lease is already correctly deleted either way; there's no
		// further durable state to retry this from.
		return
	}
	if vm.GetPhase() != poolmgrv1alpha1.VMPhase_LEASED {
		return // raced with a completed ReleaseVM/other transition
	}

	pool, err := s.store.GetPool(ctx, l.GetPoolName(), l.GetPoolNamespace())
	if err != nil {
		return
	}

	if err := EnsureVMDeleted(ctx, s.store, s.flint, vm); err != nil {
		return // left DELETING; retryPendingDeletions will pick it up next tick
	}
	FinishVMDeletion(ctx, s.store, pool, vm, s.notifier)
}
