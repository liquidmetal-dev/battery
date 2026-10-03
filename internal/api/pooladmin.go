package api

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// poolDeleteCleanupTimeout bounds DeletePool's inline flintlock deletes,
// which run detached from the RPC's own cancellation.
const poolDeleteCleanupTimeout = 30 * time.Second

// PoolLifecycle starts and stops a pool's Reconciler in response to
// CreatePool/UpdatePool/DeletePool. Satisfied structurally by
// *poolmanager.Manager; kept narrow here so internal/api doesn't need to
// import internal/poolmanager.
type PoolLifecycle interface {
	StartReconciler(spec *poolmgrv1alpha1.PoolSpec) error
	StopReconciler(name, namespace string)
	// StopReconcilerAndWait stops the pool's reconciler and waits for it,
	// and any reconciler stopped earlier for the pool, to exit, so nothing is
	// still provisioning for the pool on a nil return.
	StopReconcilerAndWait(ctx context.Context, name, namespace string) error
}

// NoopPoolLifecycle implements PoolLifecycle by doing nothing. Used when
// poolMgr is nil, e.g. in most unit tests.
type NoopPoolLifecycle struct{}

// StartReconciler does nothing and always succeeds.
func (NoopPoolLifecycle) StartReconciler(*poolmgrv1alpha1.PoolSpec) error { return nil }

// StopReconciler does nothing.
func (NoopPoolLifecycle) StopReconciler(string, string) {}

// StopReconcilerAndWait does nothing and always succeeds.
func (NoopPoolLifecycle) StopReconcilerAndWait(context.Context, string, string) error { return nil }

// PoolAdminServer implements poolmgrv1alpha1.PoolAdminServer: the CRUD
// lifecycle of pool definitions.
type PoolAdminServer struct {
	poolmgrv1alpha1.UnimplementedPoolAdminServer

	store   store.Store
	flint   *flintlockclient.Pool
	poolMgr PoolLifecycle

	poolLocksMu sync.Mutex
	poolLocks   map[poolLockKey]*sync.Mutex
}

// NewPoolAdminServer returns a PoolAdminServer backed by st, deleting the
// VMs of a deleted pool through flint. If flint is nil, DeletePool leaves
// those VMs DELETING for the Sweeper. If poolMgr is nil, NoopPoolLifecycle{}
// is used.
func NewPoolAdminServer(st store.Store, flint *flintlockclient.Pool, poolMgr PoolLifecycle) *PoolAdminServer {
	if poolMgr == nil {
		poolMgr = NoopPoolLifecycle{}
	}
	return &PoolAdminServer{store: st, flint: flint, poolMgr: poolMgr, poolLocks: make(map[poolLockKey]*sync.Mutex)}
}

// poolLockKey identifies the pool a lockPool call serializes on.
type poolLockKey struct {
	name      string
	namespace string
}

// lockPool serializes CreatePool/UpdatePool/DeletePool for the same
// (name, namespace). Without this, two concurrent RPCs against the same
// pool can interleave their store write with their PoolLifecycle
// transition: e.g. two concurrent UpdatePool calls could persist spec A
// then spec B, but call StopReconciler+StartReconciler in the order B then
// A - leaving the store holding B while the live reconciler runs against
// the stale A, permanently disagreeing until another successful
// Create/Update/Delete cycle. Locking the whole read-validate-write-
// lifecycle sequence per pool, across all three RPCs, closes that window:
// only one such sequence for a given pool can be in flight at a time.
// Returns an unlock function; callers hold it (typically via defer) for the
// duration of that pool-scoped critical section.
func (s *PoolAdminServer) lockPool(name, namespace string) func() {
	key := poolLockKey{name: name, namespace: namespace}

	s.poolLocksMu.Lock()
	l, ok := s.poolLocks[key]
	if !ok {
		l = &sync.Mutex{}
		s.poolLocks[key] = l
	}
	s.poolLocksMu.Unlock()

	l.Lock()
	return l.Unlock
}

// validatePoolSpec checks the fields CreatePool/UpdatePool both require,
// rejects a template whose network config would be duplicated across the
// pool's VMs (see validateTemplateNetwork), and forces
// spec.MicrovmTemplate.AllowGuestAgent to true: pool-managed VMs always need
// the guest-agent vsock channel for create/pre-lease hooks, regardless of
// what the caller's template set.
func validatePoolSpec(spec *poolmgrv1alpha1.PoolSpec) error {
	if spec.GetName() == "" {
		return status.Error(codes.InvalidArgument, "spec.name is required")
	}
	if spec.GetNamespace() == "" {
		return status.Error(codes.InvalidArgument, "spec.namespace is required")
	}
	if _, err := reconciler.NewStrategy(spec.GetReplenishmentStrategy()); err != nil {
		return status.Errorf(codes.InvalidArgument, "spec.replenishment_strategy: %v", err)
	}
	switch spec.GetHookFailurePolicy() {
	case poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE, poolmgrv1alpha1.HookFailurePolicy_QUARANTINE:
	default:
		return status.Errorf(codes.InvalidArgument, "spec.hook_failure_policy: invalid value %v", spec.GetHookFailurePolicy())
	}

	if spec.MicrovmTemplate == nil {
		return status.Error(codes.InvalidArgument, "spec.microvm_template is required")
	}
	if err := validateTemplateNetwork(spec); err != nil {
		return err
	}
	spec.MicrovmTemplate.AllowGuestAgent = true

	return nil
}

// validateTemplateNetwork rejects a template interface with a guest_mac or
// a static address in a pool that can hold more than one VM at a time. The
// template is sent to flintlock unchanged for every VM (Provision overrides
// only the id, namespace and allow_guest_agent), so such a field would give
// every one of them the same MAC or IP.
//
// Three settings make a pool hold more than one VM by design:
//   - a size above 1;
//   - IMMEDIATE_ON_LEASE, which provisions a new VM on every claim without
//     counting the leased ones;
//   - QUARANTINE, which keeps a VM whose hook failed. Nothing but deleting
//     the pool removes it, so the pool could never be replenished.
//
// What's left is a size <= 1 MIN_SIZE_THRESHOLD or REPLACE_ON_DELETE pool
// with DELETE_AND_REPLACE. This check only sees the spec, so for that pool
// the reconciler enforces the single VM at provision time, against the VMs
// that actually exist (see reconciler.TemplateHasStaticNetwork's caller).
func validateTemplateNetwork(spec *poolmgrv1alpha1.PoolSpec) error {
	if spec.GetSize() <= 1 &&
		spec.GetReplenishmentStrategy().GetType() != poolmgrv1alpha1.ReplenishmentStrategyType_IMMEDIATE_ON_LEASE &&
		spec.GetHookFailurePolicy() != poolmgrv1alpha1.HookFailurePolicy_QUARANTINE {
		return nil
	}

	const remedy = "leave it unset unless size is at most 1, the replenishment strategy is not IMMEDIATE_ON_LEASE and the hook failure policy is not QUARANTINE"
	for i, iface := range spec.GetMicrovmTemplate().GetInterfaces() {
		if iface.GetGuestMac() != "" {
			return status.Errorf(codes.InvalidArgument,
				"spec.microvm_template.interfaces[%d].guest_mac: a fixed MAC address would be given to every VM in the pool; %s", i, remedy)
		}
		if iface.GetAddress() != nil {
			return status.Errorf(codes.InvalidArgument,
				"spec.microvm_template.interfaces[%d].address: a static address would be given to every VM in the pool; %s (DHCP is used when it is unset)", i, remedy)
		}
	}
	return nil
}

// CreatePool validates spec, rejects a name/namespace that already exists,
// and persists the new pool. The returned Pool has zero-valued status: a
// freshly created pool has no VMs yet.
func (s *PoolAdminServer) CreatePool(ctx context.Context, req *poolmgrv1alpha1.CreatePoolRequest) (*poolmgrv1alpha1.Pool, error) {
	spec := req.GetSpec()
	if err := validatePoolSpec(spec); err != nil {
		return nil, err
	}

	log := slog.Default().With("pool", spec.GetName(), "namespace", spec.GetNamespace())
	log.InfoContext(ctx, "pooladmin: CreatePool requested")

	unlock := s.lockPool(spec.GetName(), spec.GetNamespace())
	defer unlock()

	_, err := s.store.GetPool(ctx, spec.GetName(), spec.GetNamespace())
	if err == nil {
		log.WarnContext(ctx, "pooladmin: CreatePool failed: pool already exists")
		return nil, status.Errorf(codes.AlreadyExists, "pool %s/%s already exists", spec.GetNamespace(), spec.GetName())
	}
	if !errors.Is(err, store.ErrNotFound) {
		log.ErrorContext(ctx, "pooladmin: CreatePool: get pool failed", "error", err)
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	if err := s.store.CreatePool(ctx, spec); err != nil {
		log.ErrorContext(ctx, "pooladmin: CreatePool: store write failed", "error", err)
		return nil, status.Errorf(codes.Internal, "create pool: %v", err)
	}

	// A start failure here is unreachable in practice (spec's replenishment
	// strategy was already validated above), but if it ever happens, the
	// pool itself was created successfully - failing the RPC would misreport
	// that to the caller. Log it as an operational signal instead; the pool
	// will simply have no reconciler running until poolmgrd restarts (which
	// re-seeds every pool) or the pool is deleted and recreated.
	if err := s.poolMgr.StartReconciler(spec); err != nil {
		log.ErrorContext(ctx, "pooladmin: start reconciler failed", "error", err)
	} else {
		log.InfoContext(ctx, "pooladmin: pool created")
	}

	return &poolmgrv1alpha1.Pool{Spec: spec, Status: &poolmgrv1alpha1.PoolStatus{}}, nil
}

// GetPool returns the named pool's spec plus its current live status.
func (s *PoolAdminServer) GetPool(ctx context.Context, req *poolmgrv1alpha1.GetPoolRequest) (*poolmgrv1alpha1.Pool, error) {
	return s.getPool(ctx, req.GetRef().GetName(), req.GetRef().GetNamespace())
}

// ListPools returns every pool, optionally filtered to a single namespace,
// each with its current live status.
func (s *PoolAdminServer) ListPools(ctx context.Context, req *poolmgrv1alpha1.ListPoolsRequest) (*poolmgrv1alpha1.ListPoolsResponse, error) {
	log := slog.Default()
	if req.Namespace != nil {
		log = log.With("namespace", req.GetNamespace())
	}
	log.DebugContext(ctx, "pooladmin: ListPools requested")

	specs, err := s.store.ListPools(ctx)
	if err != nil {
		log.ErrorContext(ctx, "pooladmin: ListPools failed", "error", err)
		return nil, status.Errorf(codes.Internal, "list pools: %v", err)
	}

	resp := &poolmgrv1alpha1.ListPoolsResponse{}
	for _, spec := range specs {
		if req.Namespace != nil && spec.GetNamespace() != req.GetNamespace() {
			continue
		}
		counts, err := reconciler.CountVMs(ctx, s.store, spec.GetName(), spec.GetNamespace())
		if err != nil {
			log.ErrorContext(ctx, "pooladmin: ListPools: count vms failed", "pool", spec.GetName(), "namespace", spec.GetNamespace(), "error", err)
			return nil, status.Errorf(codes.Internal, "count vms: %v", err)
		}
		resp.Pools = append(resp.Pools, &poolmgrv1alpha1.Pool{Spec: spec, Status: countsToStatus(counts)})
	}
	log.DebugContext(ctx, "pooladmin: ListPools completed", "count", len(resp.Pools))
	return resp, nil
}

// UpdatePool validates the new spec, confirms the pool it identifies already
// exists, and persists the update.
func (s *PoolAdminServer) UpdatePool(ctx context.Context, req *poolmgrv1alpha1.UpdatePoolRequest) (*poolmgrv1alpha1.Pool, error) {
	spec := req.GetSpec()
	if err := validatePoolSpec(spec); err != nil {
		return nil, err
	}

	log := slog.Default().With("pool", spec.GetName(), "namespace", spec.GetNamespace())
	log.InfoContext(ctx, "pooladmin: UpdatePool requested")

	unlock := s.lockPool(spec.GetName(), spec.GetNamespace())
	defer unlock()

	if _, err := s.store.GetPool(ctx, spec.GetName(), spec.GetNamespace()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			log.WarnContext(ctx, "pooladmin: UpdatePool failed: pool not found")
			return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", spec.GetNamespace(), spec.GetName())
		}
		log.ErrorContext(ctx, "pooladmin: UpdatePool: get pool failed", "error", err)
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	if err := s.store.UpdatePool(ctx, spec); err != nil {
		log.ErrorContext(ctx, "pooladmin: UpdatePool: store write failed", "error", err)
		return nil, status.Errorf(codes.Internal, "update pool: %v", err)
	}

	// Same log-only philosophy as CreatePool's StartReconciler call above:
	// the pool's spec is already persisted, so failing the RPC here would
	// misreport that. Stop the old reconciler (if any - StopReconciler is a
	// no-op for an unknown pool) and start a fresh one against the new spec;
	// on start failure the pool simply has no reconciler running until
	// poolmgrd restarts (re-seeds every pool) or another successful
	// CreatePool/UpdatePool/DeletePool cycle.
	s.poolMgr.StopReconciler(spec.GetName(), spec.GetNamespace())
	if err := s.poolMgr.StartReconciler(spec); err != nil {
		log.ErrorContext(ctx, "pooladmin: restart reconciler failed", "error", err)
	} else {
		log.InfoContext(ctx, "pooladmin: pool updated")
	}

	counts, err := reconciler.CountVMs(ctx, s.store, spec.GetName(), spec.GetNamespace())
	if err != nil {
		log.ErrorContext(ctx, "pooladmin: UpdatePool: count vms failed", "error", err)
		return nil, status.Errorf(codes.Internal, "count vms: %v", err)
	}
	return &poolmgrv1alpha1.Pool{Spec: spec, Status: countsToStatus(counts)}, nil
}

// DeletePool deletes the named pool and drains it: every VM the pool owns is
// deleted with it. A pool with a leased VM (LEASED or
// PRE_LEASE_HOOK_RUNNING) fails with FailedPrecondition unless req.force is
// set, in which case those VMs and their leases are deleted too.
//
// The pool's reconciler is stopped, and waited for, before the pool row is
// removed, so no in-flight Provision can add a VM afterwards. The pool row
// and its VMs' transition to DELETING then commit in one store transaction;
// once that has happened DeletePool returns OK even if a microVM could not
// be deleted from its flintlock host, since the Sweeper's pending-deletion
// retry finishes any VM left DELETING.
func (s *PoolAdminServer) DeletePool(ctx context.Context, req *poolmgrv1alpha1.DeletePoolRequest) (*emptypb.Empty, error) {
	name, ns := req.GetRef().GetName(), req.GetRef().GetNamespace()
	force := req.GetForce()

	log := slog.Default().With("pool", name, "namespace", ns)
	log.InfoContext(ctx, "pooladmin: DeletePool requested", "force", force)

	unlock := s.lockPool(name, ns)
	defer unlock()

	spec, err := s.store.GetPool(ctx, name, ns)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			log.WarnContext(ctx, "pooladmin: DeletePool failed: pool not found")
			return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", ns, name)
		}
		log.ErrorContext(ctx, "pooladmin: DeletePool: get pool failed", "error", err)
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	// Refuse early, before disturbing the reconciler, when the answer is
	// already known. DeletePoolAndMarkVMs repeats this check atomically.
	if !force {
		vms, err := s.store.ListVMsByPool(ctx, name, ns, nil)
		if err != nil {
			log.ErrorContext(ctx, "pooladmin: DeletePool: list vms failed", "error", err)
			return nil, status.Errorf(codes.Internal, "list vms: %v", err)
		}
		if n := countLeased(vms); n > 0 {
			log.WarnContext(ctx, "pooladmin: DeletePool failed: pool has leased VMs", "leased_count", n)
			return nil, status.Errorf(codes.FailedPrecondition, "pool %s/%s has %d leased VMs, release them or delete with force", ns, name, n)
		}
	}

	if err := s.poolMgr.StopReconcilerAndWait(ctx, name, ns); err != nil {
		log.ErrorContext(ctx, "pooladmin: DeletePool: reconciler did not stop", "error", err)
		s.restartReconciler(ctx, log, spec)
		return nil, status.FromContextError(err).Err()
	}

	vms, err := s.store.DeletePoolAndMarkVMs(ctx, name, ns, force)
	switch {
	case errors.Is(err, store.ErrPoolHasLeasedVMs):
		// A ClaimVM landed between the check above and the transaction.
		log.WarnContext(ctx, "pooladmin: DeletePool failed: pool has leased VMs")
		s.restartReconciler(ctx, log, spec)
		return nil, status.Errorf(codes.FailedPrecondition, "pool %s/%s has leased VMs, release them or delete with force", ns, name)
	case errors.Is(err, store.ErrNotFound):
		log.WarnContext(ctx, "pooladmin: DeletePool failed: pool not found")
		return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", ns, name)
	case err != nil && ctx.Err() != nil:
		// The client went away or its deadline passed before the
		// transaction committed; nothing was changed.
		log.WarnContext(ctx, "pooladmin: DeletePool abandoned by caller", "error", err)
		s.restartReconciler(ctx, log, spec)
		return nil, status.FromContextError(ctx.Err()).Err()
	case err != nil:
		log.ErrorContext(ctx, "pooladmin: DeletePool: store delete failed", "error", err)
		s.restartReconciler(ctx, log, spec)
		return nil, status.Errorf(codes.Internal, "delete pool: %v", err)
	}

	s.deletePoolVMs(ctx, log, spec, vms)

	log.InfoContext(ctx, "pooladmin: pool deleted", "vm_count", len(vms))
	return &emptypb.Empty{}, nil
}

// countLeased returns how many of vms are held by a consumer, or about to
// be: LEASED, or PRE_LEASE_HOOK_RUNNING (a ClaimVM in flight).
func countLeased(vms []*poolmgrv1alpha1.VMRecord) int {
	n := 0
	for _, vm := range vms {
		switch vm.GetPhase() {
		case poolmgrv1alpha1.VMPhase_LEASED, poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING:
			n++
		}
	}
	return n
}

// restartReconciler starts spec's reconciler again after a DeletePool that
// stopped it but then left the pool in place. As in CreatePool, a start
// failure is only logged: the RPC is already failing for its own reason.
func (s *PoolAdminServer) restartReconciler(ctx context.Context, log *slog.Logger, spec *poolmgrv1alpha1.PoolSpec) {
	if err := s.poolMgr.StartReconciler(spec); err != nil {
		log.ErrorContext(ctx, "pooladmin: restart reconciler failed", "error", err)
	}
}

// deletePoolVMs emits VM_DELETED_ON_POOL_DELETE for, and deletes, each VM of
// a pool that DeletePoolAndMarkVMs has just removed. It runs on a context
// detached from ctx's cancellation: the pool is already gone, so a client
// that hangs up must not stop its microVMs being deleted. A failed delete is
// only logged; the VM stays DELETING for the Sweeper to retry.
//
// Every event is emitted before any flintlock call, and the deletes run
// concurrently, each bounded by its own poolDeleteCleanupTimeout: the
// Sweeper emits nothing for a VM whose pool is gone, so an unresponsive
// host must not be able to cost the pool's other VMs their event, or hold
// up their deletion.
func (s *PoolAdminServer) deletePoolVMs(ctx context.Context, log *slog.Logger, spec *poolmgrv1alpha1.PoolSpec, vms []*poolmgrv1alpha1.VMRecord) {
	ctx = context.WithoutCancel(ctx)

	eventCtx, cancel := context.WithTimeout(ctx, poolDeleteCleanupTimeout)
	for _, vm := range vms {
		reconciler.EmitEvent(eventCtx, s.store, spec, vm.GetUid(), poolmgrv1alpha1.EventType_VM_DELETED_ON_POOL_DELETE)
	}
	cancel()

	if s.flint == nil {
		return
	}

	var wg sync.WaitGroup
	for _, vm := range vms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deleteCtx, cancel := context.WithTimeout(ctx, poolDeleteCleanupTimeout)
			defer cancel()
			err := reconciler.EnsureVMDeleted(deleteCtx, s.store, s.flint, vm)
			// A missing row means the Sweeper's own retry got to this
			// DELETING VM first: it is deleted, which is all that was wanted.
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				log.WarnContext(ctx, "pooladmin: DeletePool: microvm delete failed, leaving it for the sweeper", "microvm_uid", vm.GetUid(), "error", err)
			}
		}()
	}
	wg.Wait()
}

func (s *PoolAdminServer) getPool(ctx context.Context, name, namespace string) (*poolmgrv1alpha1.Pool, error) {
	log := slog.Default().With("pool", name, "namespace", namespace)
	log.DebugContext(ctx, "pooladmin: GetPool requested")

	spec, err := s.store.GetPool(ctx, name, namespace)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			log.DebugContext(ctx, "pooladmin: GetPool failed: pool not found")
			return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", namespace, name)
		}
		log.ErrorContext(ctx, "pooladmin: GetPool: get pool failed", "error", err)
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	counts, err := reconciler.CountVMs(ctx, s.store, name, namespace)
	if err != nil {
		log.ErrorContext(ctx, "pooladmin: GetPool: count vms failed", "error", err)
		return nil, status.Errorf(codes.Internal, "count vms: %v", err)
	}
	return &poolmgrv1alpha1.Pool{Spec: spec, Status: countsToStatus(counts)}, nil
}

func countsToStatus(c reconciler.VMCounts) *poolmgrv1alpha1.PoolStatus {
	return &poolmgrv1alpha1.PoolStatus{
		AvailableCount:    int32(c.Available),
		LeasedCount:       int32(c.Leased),
		ProvisioningCount: int32(c.Provisioning),
		QuarantinedCount:  int32(c.Quarantined),
	}
}
