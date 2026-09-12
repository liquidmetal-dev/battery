// Package api implements the pool manager's gRPC service handlers.
package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// ReconcilerNotifier lets the Lease service nudge whichever component owns a
// pool's Reconciler after a claim or a VM deletion, without the Lease
// service needing to know how pools are wired to their Reconcilers (that
// wiring doesn't exist yet - it's left to a future manager). Inject
// NoopNotifier when no reconciler is running, e.g. in most unit tests.
type ReconcilerNotifier interface {
	NotifyVMClaimed(poolName, poolNamespace string)
	NotifyVMDeleted(poolName, poolNamespace string)
}

// NoopNotifier implements ReconcilerNotifier by doing nothing.
type NoopNotifier struct{}

// NotifyVMClaimed does nothing.
func (NoopNotifier) NotifyVMClaimed(string, string) {}

// NotifyVMDeleted does nothing.
func (NoopNotifier) NotifyVMDeleted(string, string) {}

// DefaultCleanupTimeout bounds hook-failure cleanup when HookExecConfig
// doesn't set CleanupTimeout.
const DefaultCleanupTimeout = 30 * time.Second

// HookExecConfig bounds pre-lease-hook execution, mirroring the
// exec-related fields of reconciler.ProvisionConfig.
type HookExecConfig struct {
	// ExecTimeoutSeconds bounds each pre_lease_command's server-side run
	// time. 0 means no server-side timeout.
	ExecTimeoutSeconds int32
	// CleanupTimeout bounds hook-failure cleanup (quarantine/delete via
	// reconciler.ApplyHookFailurePolicy) after ClaimAvailableVM has
	// committed. Cleanup runs on a context detached from the RPC's own
	// context (see applyHookFailurePolicy) so a caller cancelling or the
	// RPC deadline expiring can't strand a VM LEASED/PRE_LEASE_HOOK_RUNNING
	// with no lease; this timeout bounds that detached cleanup instead.
	// Zero uses DefaultCleanupTimeout.
	CleanupTimeout time.Duration
}

// LeaseServer implements poolmgrv1alpha1.LeaseServer: ClaimVM, Heartbeat,
// and ReleaseVM.
type LeaseServer struct {
	poolmgrv1alpha1.UnimplementedLeaseServer

	store    store.Store
	flint    *flintlockclient.Pool
	cfg      HookExecConfig
	notifier ReconcilerNotifier
	metrics  *metrics.Registry
}

// NewLeaseServer returns a LeaseServer backed by st and flint. If notifier
// is nil, NoopNotifier{} is used. If m is nil, a fresh unshared
// metrics.Registry is used (see reconciler.NewProvisioner).
func NewLeaseServer(st store.Store, flint *flintlockclient.Pool, cfg HookExecConfig, notifier ReconcilerNotifier, m *metrics.Registry) *LeaseServer {
	if notifier == nil {
		notifier = NoopNotifier{}
	}
	if cfg.CleanupTimeout <= 0 {
		cfg.CleanupTimeout = DefaultCleanupTimeout
	}
	if m == nil {
		m = metrics.NewRegistry()
	}
	return &LeaseServer{store: st, flint: flint, cfg: cfg, notifier: notifier, metrics: m}
}

// ClaimVM atomically claims an AVAILABLE VM from the named pool, runs the
// pool's pre_lease_commands, and creates a Lease. It fails with
// RESOURCE_EXHAUSTED when no VM is AVAILABLE.
func (s *LeaseServer) ClaimVM(ctx context.Context, req *poolmgrv1alpha1.ClaimVMRequest) (*poolmgrv1alpha1.ClaimVMResponse, error) {
	poolName, poolNS := req.GetPool().GetName(), req.GetPool().GetNamespace()

	pool, err := s.store.GetPool(ctx, poolName, poolNS)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", poolNS, poolName)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	vm, err := s.store.ClaimAvailableVM(ctx, poolName, poolNS)
	if errors.Is(err, store.ErrNoAvailableVM) {
		return nil, status.Errorf(codes.ResourceExhausted, "no available vm in pool %s/%s", poolNS, poolName)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "claim available vm: %v", err)
	}
	// From here on, vm is already persisted as LEASED with no lease row yet.
	// Any failure below applies pool.HookFailurePolicy (via
	// reconciler.ApplyHookFailurePolicy) so the VM never sits claimed with
	// no corresponding lease.

	if err := s.runPreLeaseHooks(ctx, pool, vm); err != nil {
		return nil, status.Errorf(codes.Internal, "pre-lease hook: %v", err)
	}

	leaseID := uuid.NewString()
	now := time.Now()
	expiresAt := now.Add(pool.GetHeartbeatExpiryThreshold().AsDuration())

	vm.Phase = poolmgrv1alpha1.VMPhase_LEASED
	vm.LeaseId = &leaseID
	vm.UpdatedAt = timestamppb.New(now)
	if err := s.store.UpdateVM(ctx, vm); err != nil {
		s.applyHookFailurePolicy(ctx, pool, vm)
		return nil, status.Errorf(codes.Internal, "update vm to leased: %v", err)
	}

	lease := &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         leaseID,
		VmUid:           vm.GetUid(),
		PoolName:        poolName,
		PoolNamespace:   poolNS,
		ClaimedAt:       timestamppb.New(now),
		LastHeartbeatAt: timestamppb.New(now),
		ExpiresAt:       timestamppb.New(expiresAt),
	}
	if err := s.store.CreateLease(ctx, lease); err != nil {
		s.applyHookFailurePolicy(ctx, pool, vm)
		return nil, status.Errorf(codes.Internal, "create lease: %v", err)
	}

	reconciler.EmitEvent(ctx, s.store, pool, vm.GetUid(), poolmgrv1alpha1.EventType_VM_CLAIMED)
	s.metrics.RecordVMClaim(poolName, poolNS)
	s.notifier.NotifyVMClaimed(poolName, poolNS)

	// Best-effort: the lease is already committed at this point, so a
	// failure to fetch network interfaces just means an empty map in the
	// response - the caller can still Heartbeat/ReleaseVM successfully.
	var netIfaces map[string]*flintlocktypes.NetworkInterfaceStatus
	if client, cerr := s.flint.Client(vm.GetFlintlockHost()); cerr == nil {
		if resp, gerr := client.GetMicroVM(ctx, &microvmv1alpha1.GetMicroVMRequest{Uid: vm.GetUid()}); gerr == nil {
			netIfaces = resp.GetMicrovm().GetStatus().GetNetworkInterfaces()
		}
	}

	hostName := vm.GetFlintlockHost()
	addr, _ := s.flint.Address(hostName)

	return &poolmgrv1alpha1.ClaimVMResponse{
		LeaseId:           leaseID,
		VmUid:             vm.GetUid(),
		NetworkInterfaces: netIfaces,
		Host: &poolmgrv1alpha1.HostInfo{
			Name:    hostName,
			Address: addr,
		},
	}, nil
}

// runPreLeaseHooks transitions vm to PRE_LEASE_HOOK_RUNNING and executes
// pool.GetPreLeaseCommands() via the VM's flintlock exec client, mirroring
// reconciler.Provisioner.Provision's create-command loop. On any failure it
// applies pool.HookFailurePolicy (which also emits VM_HOOK_FAILED) and
// returns a wrapped error; no lease is created in that case.
func (s *LeaseServer) runPreLeaseHooks(ctx context.Context, pool *poolmgrv1alpha1.PoolSpec, vm *poolmgrv1alpha1.VMRecord) error {
	vm.Phase = poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING
	vm.UpdatedAt = timestamppb.Now()
	if err := s.store.UpdateVM(ctx, vm); err != nil {
		s.applyHookFailurePolicy(ctx, pool, vm)
		return fmt.Errorf("update vm phase: %w", err)
	}

	execClient, err := s.flint.ExecClient(vm.GetFlintlockHost())
	if err != nil {
		s.applyHookFailurePolicy(ctx, pool, vm)
		return fmt.Errorf("exec client: %w", err)
	}

	start := time.Now()
	for _, cmd := range pool.GetPreLeaseCommands() {
		result, err := flintlockclient.Exec(ctx, execClient, vm.GetUid(), cmd, flintlockclient.ExecOptions{TimeoutSeconds: s.cfg.ExecTimeoutSeconds})
		if err != nil {
			s.applyHookFailurePolicy(ctx, pool, vm)
			return fmt.Errorf("exec %q: %w", cmd, err)
		}
		if result.ExitCode != 0 {
			s.applyHookFailurePolicy(ctx, pool, vm)
			return fmt.Errorf("exec %q: exit code %d", cmd, result.ExitCode)
		}
	}
	s.metrics.ObserveHookDuration("pre_lease", pool.GetName(), pool.GetNamespace(), time.Since(start))
	return nil
}

// applyHookFailurePolicy applies pool.HookFailurePolicy to vm (quarantine or
// delete) and, if the policy actually deleted the VM, notifies so
// REPLACE_ON_DELETE pools can replenish - reconciler.ApplyHookFailurePolicy
// itself has no notifier, so this wraps it for every ClaimVM call site.
//
// Cleanup runs on a context detached from ctx (context.WithoutCancel) and
// bounded by s.cfg.CleanupTimeout, not on ctx itself: by the time this is
// called, ClaimAvailableVM has already committed the VM as claimed, so if
// the RPC's own context is cancelled or its deadline expires (the caller
// disconnected, or cancelled after a hook failure), cleanup must still be
// able to quarantine or delete the VM rather than leaving it stranded
// LEASED/PRE_LEASE_HOOK_RUNNING with no lease and nothing to retry it.
func (s *LeaseServer) applyHookFailurePolicy(ctx context.Context, pool *poolmgrv1alpha1.PoolSpec, vm *poolmgrv1alpha1.VMRecord) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.CleanupTimeout)
	defer cancel()

	reconciler.ApplyHookFailurePolicy(cleanupCtx, s.store, s.flint, pool, vm, "pre_lease", s.metrics)
	if pool.GetHookFailurePolicy() != poolmgrv1alpha1.HookFailurePolicy_QUARANTINE {
		s.notifier.NotifyVMDeleted(pool.GetName(), pool.GetNamespace())
	}
}

// Heartbeat extends leaseID's expiry by the lease's pool's
// heartbeat_expiry_threshold.
func (s *LeaseServer) Heartbeat(ctx context.Context, req *poolmgrv1alpha1.HeartbeatRequest) (*poolmgrv1alpha1.HeartbeatResponse, error) {
	lease, err := s.store.GetLease(ctx, req.GetLeaseId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "lease %s not found", req.GetLeaseId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get lease: %v", err)
	}

	pool, err := s.store.GetPool(ctx, lease.GetPoolName(), lease.GetPoolNamespace())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", lease.GetPoolNamespace(), lease.GetPoolName())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(pool.GetHeartbeatExpiryThreshold().AsDuration())
	if err := s.store.UpdateLeaseHeartbeat(ctx, req.GetLeaseId(), now, expiresAt); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "lease %s not found", req.GetLeaseId())
		}
		return nil, status.Errorf(codes.Internal, "update lease heartbeat: %v", err)
	}
	return &poolmgrv1alpha1.HeartbeatResponse{ExpiresAt: timestamppb.New(expiresAt)}, nil
}

// ReleaseVM ends leaseID's lease: the VM is deleted via flintlock and the
// lease/VM rows are removed. If flintlock doesn't confirm the deletion (the
// host is unreachable, etc.), the VM is left DELETING and this returns
// Unavailable rather than reporting success or dropping the lease row - the
// Sweeper's pending-deletion retry (or a client retry of ReleaseVM) finishes
// the job once flintlock is reachable again.
func (s *LeaseServer) ReleaseVM(ctx context.Context, req *poolmgrv1alpha1.ReleaseVMRequest) (*emptypb.Empty, error) {
	lease, err := s.store.GetLease(ctx, req.GetLeaseId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "lease %s not found", req.GetLeaseId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get lease: %v", err)
	}

	vm, err := s.store.GetVM(ctx, lease.GetVmUid())
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "get vm: %v", err)
	}

	if vm != nil {
		if err := reconciler.EnsureVMDeleted(ctx, s.store, s.flint, vm); err != nil {
			return nil, status.Errorf(codes.Unavailable, "vm cleanup pending, retry later: %v", err)
		}
		pool, perr := s.store.GetPool(ctx, lease.GetPoolName(), lease.GetPoolNamespace())
		if perr != nil {
			return nil, status.Errorf(codes.Internal, "get pool: %v", perr)
		}
		reconciler.FinishVMDeletion(ctx, s.store, pool, vm, s.notifier, s.metrics)
		return &emptypb.Empty{}, nil
	}

	// VM record already gone (a previous attempt already finished the
	// deletion): just make sure the lease row is gone too, for idempotency.
	if err := s.store.DeleteLease(ctx, req.GetLeaseId()); err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "delete lease: %v", err)
	}
	s.notifier.NotifyVMDeleted(lease.GetPoolName(), lease.GetPoolNamespace())
	return &emptypb.Empty{}, nil
}

// ListLeases returns every lease, optionally filtered to one pool. Returns only persisted
// LeaseRecord fields - no flintlock lookups, unlike ClaimVM's response.
func (s *LeaseServer) ListLeases(ctx context.Context, req *poolmgrv1alpha1.ListLeasesRequest) (*poolmgrv1alpha1.ListLeasesResponse, error) {
	leases, err := s.store.ListLeases(ctx, req.GetPoolRef())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list leases: %v", err)
	}
	return &poolmgrv1alpha1.ListLeasesResponse{Leases: leases}, nil
}
