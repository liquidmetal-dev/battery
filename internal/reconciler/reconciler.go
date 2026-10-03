package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// DefaultTickInterval is how often a Reconciler re-evaluates its pool's
// desired VM count when the caller doesn't specify one.
const DefaultTickInterval = 10 * time.Second

// notifyBuffer is the size of the claimed/deleted notification channels.
// Notifications are coalescing signals, not a queue of individual events:
// a full buffer means a reconcile is already pending, and since that
// reconcile provisions the pool's whole shortfall rather than one VM per
// notification, further notifications before it runs are redundant.
const notifyBuffer = 1

// Reconciler runs the control loop for a single pool: once at start, on
// every tick, and on every claim/delete notification, it counts the pool's
// VMs, asks the pool's Strategy how many new ones are needed and provisions
// them.
type Reconciler struct {
	pool         *poolmgrv1alpha1.PoolSpec
	store        store.Store
	strategy     Strategy
	provisioner  *Provisioner
	tickInterval time.Duration
	log          *slog.Logger

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
		log:          slog.Default().With("pool", pool.GetName(), "namespace", pool.GetNamespace()),
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
// ctx.Err(). At start, on each tick and on each notification it reconciles:
// computes how many VMs to provision and starts that many Provision calls
// concurrently; one failed Provision is logged and does not stop the others
// or the loop, and the next reconcile retries it.
func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.tickInterval)
	defer ticker.Stop()

	r.log.InfoContext(ctx, "reconciler: started", "tick_interval", r.tickInterval)
	defer r.log.InfoContext(ctx, "reconciler: stopped")

	// Reconcile straight away rather than waiting out the first tick, so a
	// fresh pool starts filling as soon as its reconciler does.
	r.reconcile(ctx, r.strategy.DesiredNewVMs)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.reconcile(ctx, r.strategy.DesiredNewVMs)
		case <-r.claimed:
			r.reconcile(ctx, r.strategy.OnVMClaimed)
		case <-r.deleted:
			r.reconcile(ctx, r.strategy.OnVMDeleted)
		}
	}
}

// reconcile counts the pool's VMs and provisions however many desired, one
// of the Strategy's hooks, asks for. If counting fails nothing is
// provisioned; the next tick tries again.
func (r *Reconciler) reconcile(ctx context.Context, desired func(*poolmgrv1alpha1.PoolSpec, VMCounts) int) {
	counts, err := r.countVMs(ctx)
	if err != nil {
		r.log.ErrorContext(ctx, "reconciler: failed to count VMs", "error", err)
		return
	}
	r.provisionN(ctx, desired(r.pool, counts))
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
	// Leases are read after the VMs, so a lease committed in between is seen
	// and its VM counts as leased; read the other way round, a claim that
	// commits between the two reads would look pending.
	leases, err := st.ListLeases(ctx, &poolmgrv1alpha1.PoolRef{Name: poolName, Namespace: poolNamespace})
	if err != nil {
		return VMCounts{}, fmt.Errorf("reconciler: ListLeases: %w", err)
	}
	committed := make(map[string]struct{}, len(leases))
	for _, lease := range leases {
		committed[lease.GetLeaseId()] = struct{}{}
	}

	var counts VMCounts
	for _, vm := range vms {
		switch vm.GetPhase() {
		case poolmgrv1alpha1.VMPhase_AVAILABLE:
			counts.Available++
		case poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING:
			// A transient phase before a VM is handed to a consumer. It
			// must be counted somewhere, or MIN_SIZE_THRESHOLD would see it
			// as neither available nor in-flight and over-provision.
			counts.Claiming++
		case poolmgrv1alpha1.VMPhase_LEASED:
			// store.ClaimAvailableVM marks a VM LEASED well before its claim
			// is final. Only the lease row itself, which store.LeaseVM
			// writes together with the VM's lease id, proves the claim
			// committed; without one the claim is still pending and can
			// hand the VM back. (One left that way by a dead process is
			// cleared by RecoverAbandonedClaims.)
			if _, ok := committed[vm.GetLeaseId()]; ok {
				counts.Leased++
			} else {
				counts.Claiming++
			}
		case poolmgrv1alpha1.VMPhase_PROVISIONING, poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING:
			counts.Provisioning++
		case poolmgrv1alpha1.VMPhase_QUARANTINED:
			counts.Quarantined++
		}
	}
	return counts, nil
}

// TemplateHasStaticNetwork reports whether any of template's interfaces sets
// a guest_mac or a static address. Provision sends the template unchanged
// for every VM, so two VMs created from such a template share that MAC or
// address.
func TemplateHasStaticNetwork(template *flintlocktypes.MicroVMSpec) bool {
	for _, iface := range template.GetInterfaces() {
		if iface.GetGuestMac() != "" || iface.GetAddress() != nil {
			return true
		}
	}
	return false
}

// capForStaticNetwork returns how many VMs a pool whose template has static
// network config may provision right now: one if the pool has no VM record
// at all, otherwise none.
//
// The Strategy can't be trusted for this. VMCounts leaves out DELETING and
// FAILED VMs and no strategy counts QUARANTINED ones, yet all three may
// still be running on their host with the template's address. So this counts
// records in every phase, which also keeps a pool stored before the API
// rejected such a spec down to a single VM.
func (r *Reconciler) capForStaticNetwork(ctx context.Context) int {
	vms, err := r.store.ListVMsByPool(ctx, r.pool.GetName(), r.pool.GetNamespace(), nil)
	if err != nil {
		r.log.ErrorContext(ctx, "reconciler: failed to list VMs for static-network check", "error", err)
		return 0
	}
	if len(vms) > 0 {
		r.log.DebugContext(ctx, "reconciler: template has static network config, waiting for existing VMs to be removed", "existing", len(vms))
		return 0
	}
	return 1
}

// provisionN starts n Provision calls concurrently and logs any failures.
// It does not block the caller past all of them completing or ctx being
// done, whichever comes first. For a template with static network config n
// is first capped by capForStaticNetwork.
func (r *Reconciler) provisionN(ctx context.Context, n int) {
	if n > 0 && TemplateHasStaticNetwork(r.pool.GetMicrovmTemplate()) {
		n = r.capForStaticNetwork(ctx)
	}
	if n <= 0 {
		return
	}

	r.log.InfoContext(ctx, "reconciler: provisioning VMs", "count", n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := r.provisioner.Provision(ctx, r.pool); err != nil {
				if errors.Is(err, ErrNoEligibleHost) {
					// Every host available to the pool is cordoned: an expected
					// steady state during host maintenance, not a failure, so
					// it's logged quietly and this cycle is simply skipped
					// rather than escalated like a real provision error.
					r.log.DebugContext(ctx, "reconciler: no eligible host to provision on (all hosts cordoned?)")
					return
				}
				r.log.ErrorContext(ctx, "reconciler: provision failed", "error", err)
			}
		}()
	}
	wg.Wait()
}

// RecoverAbandonedClaims marks DELETING every VM, in any pool, that is
// claimed with no lease to show for it (PRE_LEASE_HOOK_RUNNING, or LEASED
// with no lease row), and returns how many it marked. The Sweeper's
// pending-deletion scan then removes them.
//
// It is only safe to call while no ClaimVM and no Sweeper can be running,
// i.e. at poolmgrd startup before anything is served: only then is such a VM
// certain to have been left by a previous process, one that died part-way
// through a claim or between ending a lease and deleting its VM. Nothing
// else would ever move it on, and since CountVMs counts it as a pending
// claim, an IMMEDIATE_ON_LEASE pool would go on treating it as warm.
//
// The VM is deleted rather than returned to AVAILABLE: its pre-lease hooks
// may have partly run, or a consumer may already have used it.
func RecoverAbandonedClaims(ctx context.Context, st store.Store) (int, error) {
	leases, err := st.ListLeases(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("reconciler: ListLeases: %w", err)
	}
	committed := make(map[string]struct{}, len(leases))
	for _, lease := range leases {
		committed[lease.GetLeaseId()] = struct{}{}
	}

	recovered := 0
	for _, phase := range []poolmgrv1alpha1.VMPhase{
		poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING,
		poolmgrv1alpha1.VMPhase_LEASED,
	} {
		vms, err := st.ListVMsByPhase(ctx, phase)
		if err != nil {
			return recovered, fmt.Errorf("reconciler: ListVMsByPhase: %w", err)
		}
		for _, vm := range vms {
			if _, ok := committed[vm.GetLeaseId()]; ok {
				continue
			}
			slog.WarnContext(ctx, "reconciler: deleting microvm left claimed with no lease",
				"pool", vm.GetPoolName(), "namespace", vm.GetPoolNamespace(), "microvm_uid", vm.GetUid(), "phase", vm.GetPhase())
			vm.Phase = poolmgrv1alpha1.VMPhase_DELETING
			vm.UpdatedAt = timestamppb.Now()
			if err := st.UpdateVM(ctx, vm); err != nil {
				return recovered, fmt.Errorf("reconciler: mark vm %s deleting: %w", vm.GetUid(), err)
			}
			recovered++
		}
	}
	return recovered, nil
}
