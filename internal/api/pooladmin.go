package api

import (
	"context"
	"errors"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// PoolAdminServer implements poolmgrv1alpha1.PoolAdminServer: the CRUD
// lifecycle of pool definitions.
type PoolAdminServer struct {
	poolmgrv1alpha1.UnimplementedPoolAdminServer

	store store.Store
}

// NewPoolAdminServer returns a PoolAdminServer backed by st.
func NewPoolAdminServer(st store.Store) *PoolAdminServer {
	return &PoolAdminServer{store: st}
}

// validatePoolSpec checks the fields CreatePool/UpdatePool both require, and
// forces spec.MicrovmTemplate.AllowGuestAgent to true: pool-managed VMs
// always need the guest-agent vsock channel for create/pre-lease hooks,
// regardless of what the caller's template set.
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
	if spec.GetHookFailurePolicy() == poolmgrv1alpha1.HookFailurePolicy_HOOK_FAILURE_POLICY_UNSPECIFIED {
		return status.Error(codes.InvalidArgument, "spec.hook_failure_policy is required")
	}

	if spec.MicrovmTemplate == nil {
		return status.Error(codes.InvalidArgument, "spec.microvm_template is required")
	}
	spec.MicrovmTemplate.AllowGuestAgent = true

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

	_, err := s.store.GetPool(ctx, spec.GetName(), spec.GetNamespace())
	if err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "pool %s/%s already exists", spec.GetNamespace(), spec.GetName())
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	if err := s.store.CreatePool(ctx, spec); err != nil {
		return nil, status.Errorf(codes.Internal, "create pool: %v", err)
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
	specs, err := s.store.ListPools(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list pools: %v", err)
	}

	resp := &poolmgrv1alpha1.ListPoolsResponse{}
	for _, spec := range specs {
		if req.Namespace != nil && spec.GetNamespace() != req.GetNamespace() {
			continue
		}
		counts, err := reconciler.CountVMs(ctx, s.store, spec.GetName(), spec.GetNamespace())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "count vms: %v", err)
		}
		resp.Pools = append(resp.Pools, &poolmgrv1alpha1.Pool{Spec: spec, Status: countsToStatus(counts)})
	}
	return resp, nil
}

// UpdatePool validates the new spec, confirms the pool it identifies already
// exists, and persists the update.
func (s *PoolAdminServer) UpdatePool(ctx context.Context, req *poolmgrv1alpha1.UpdatePoolRequest) (*poolmgrv1alpha1.Pool, error) {
	spec := req.GetSpec()
	if err := validatePoolSpec(spec); err != nil {
		return nil, err
	}

	if _, err := s.store.GetPool(ctx, spec.GetName(), spec.GetNamespace()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", spec.GetNamespace(), spec.GetName())
		}
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	if err := s.store.UpdatePool(ctx, spec); err != nil {
		return nil, status.Errorf(codes.Internal, "update pool: %v", err)
	}

	counts, err := reconciler.CountVMs(ctx, s.store, spec.GetName(), spec.GetNamespace())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "count vms: %v", err)
	}
	return &poolmgrv1alpha1.Pool{Spec: spec, Status: countsToStatus(counts)}, nil
}

// DeletePool deletes the named pool. It fails with FailedPrecondition if the
// pool still owns any VMRecord (in any phase): there's no VM-level admin API
// yet to drain a pool first, so allowing deletion here would orphan those
// VM/lease rows.
func (s *PoolAdminServer) DeletePool(ctx context.Context, req *poolmgrv1alpha1.DeletePoolRequest) (*emptypb.Empty, error) {
	name, ns := req.GetRef().GetName(), req.GetRef().GetNamespace()

	if _, err := s.store.GetPool(ctx, name, ns); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", ns, name)
		}
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	counts, err := reconciler.CountVMs(ctx, s.store, name, ns)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "count vms: %v", err)
	}
	if counts.Available+counts.Leased+counts.Provisioning+counts.Quarantined > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "pool %s/%s still has VMs, delete or drain them first", ns, name)
	}

	if err := s.store.DeletePool(ctx, name, ns); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", ns, name)
		}
		return nil, status.Errorf(codes.Internal, "delete pool: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *PoolAdminServer) getPool(ctx context.Context, name, namespace string) (*poolmgrv1alpha1.Pool, error) {
	spec, err := s.store.GetPool(ctx, name, namespace)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", namespace, name)
		}
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	counts, err := reconciler.CountVMs(ctx, s.store, name, namespace)
	if err != nil {
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
