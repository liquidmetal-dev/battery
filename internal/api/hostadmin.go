package api

import (
	"context"
	"errors"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liquidmetal-dev/battery/internal/store"
)

// HostAdminServer implements poolmgrv1alpha1.HostAdminServer: the cordon
// state of registered flintlock hosts.
type HostAdminServer struct {
	poolmgrv1alpha1.UnimplementedHostAdminServer

	store store.Store
}

// NewHostAdminServer returns a HostAdminServer backed by st.
func NewHostAdminServer(st store.Store) *HostAdminServer {
	return &HostAdminServer{store: st}
}

// CordonHost marks name as cordoned: the reconciler stops placing new VMs
// there, while existing VMs are unaffected. Cordoning an already-cordoned host
// is idempotent and just updates reason. Fails with FailedPrecondition if
// name isn't a registered host.
func (s *HostAdminServer) CordonHost(ctx context.Context, req *poolmgrv1alpha1.CordonHostRequest) (*poolmgrv1alpha1.Host, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	host, err := s.store.SetHostCordoned(ctx, req.GetName(), true, req.GetReason())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "host %q is not registered", req.GetName())
		}
		return nil, status.Errorf(codes.Internal, "cordon host: %v", err)
	}
	return host, nil
}

// UncordonHost clears name's cordoned state, resuming placement. Uncordoning a
// host that isn't cordoned is idempotent. Fails with FailedPrecondition if
// name isn't a registered host.
func (s *HostAdminServer) UncordonHost(ctx context.Context, req *poolmgrv1alpha1.UncordonHostRequest) (*poolmgrv1alpha1.Host, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	host, err := s.store.SetHostCordoned(ctx, req.GetName(), false, "")
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "host %q is not registered", req.GetName())
		}
		return nil, status.Errorf(codes.Internal, "uncordon host: %v", err)
	}
	return host, nil
}

// ListHosts returns every registered host, each with the number of VM
// records in any phase plus in-flight placements still counted against it,
// across all pools. A cordoned host at 0 is safe to take down.
func (s *HostAdminServer) ListHosts(ctx context.Context, _ *poolmgrv1alpha1.ListHostsRequest) (*poolmgrv1alpha1.ListHostsResponse, error) {
	hosts, err := s.store.ListHosts(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list hosts: %v", err)
	}

	resp := &poolmgrv1alpha1.ListHostsResponse{}
	for _, h := range hosts {
		count, err := s.store.CountVMsByHost(ctx, h.GetName())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "count vms for host %q: %v", h.GetName(), err)
		}
		resp.Hosts = append(resp.Hosts, &poolmgrv1alpha1.HostStatus{Host: h, VmCount: count})
	}
	return resp, nil
}
