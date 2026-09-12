package api

import (
	"context"
	"errors"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liquidmetal-dev/battery/internal/store"
)

// HostAdminServer implements poolmgrv1alpha1.HostAdminServer: the drain
// state of registered flintlock hosts.
type HostAdminServer struct {
	poolmgrv1alpha1.UnimplementedHostAdminServer

	store store.Store
}

// NewHostAdminServer returns a HostAdminServer backed by st.
func NewHostAdminServer(st store.Store) *HostAdminServer {
	return &HostAdminServer{store: st}
}

// DrainHost marks name as drained: the reconciler stops placing new VMs
// there, while existing VMs are unaffected. Draining an already-drained host
// is idempotent and just updates reason. Fails with FailedPrecondition if
// name isn't a registered host.
func (s *HostAdminServer) DrainHost(ctx context.Context, req *poolmgrv1alpha1.DrainHostRequest) (*poolmgrv1alpha1.Host, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	host, err := s.store.SetHostDrained(ctx, req.GetName(), true, req.GetReason())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "host %q is not registered", req.GetName())
		}
		return nil, status.Errorf(codes.Internal, "drain host: %v", err)
	}
	return host, nil
}

// UndrainHost clears name's drained state, resuming placement. Undraining a
// host that isn't drained is idempotent. Fails with FailedPrecondition if
// name isn't a registered host.
func (s *HostAdminServer) UndrainHost(ctx context.Context, req *poolmgrv1alpha1.UndrainHostRequest) (*poolmgrv1alpha1.Host, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	host, err := s.store.SetHostDrained(ctx, req.GetName(), false, "")
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "host %q is not registered", req.GetName())
		}
		return nil, status.Errorf(codes.Internal, "undrain host: %v", err)
	}
	return host, nil
}

// ListHosts returns every registered host, each with its current active
// (non-terminal) VM count across all pools.
func (s *HostAdminServer) ListHosts(ctx context.Context, _ *poolmgrv1alpha1.ListHostsRequest) (*poolmgrv1alpha1.ListHostsResponse, error) {
	hosts, err := s.store.ListHosts(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list hosts: %v", err)
	}

	resp := &poolmgrv1alpha1.ListHostsResponse{}
	for _, h := range hosts {
		count, err := s.store.CountActiveVMsByHost(ctx, h.GetName())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "count active vms for host %q: %v", h.GetName(), err)
		}
		resp.Hosts = append(resp.Hosts, &poolmgrv1alpha1.HostStatus{Host: h, ActiveVmCount: count})
	}
	return resp, nil
}
