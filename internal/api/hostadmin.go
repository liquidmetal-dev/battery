package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// hostCheckTimeout bounds AddHost's and UpdateHost's version check, so an
// address that silently drops packets fails the RPC instead of hanging it
// until the caller's own deadline, if it has one.
const hostCheckTimeout = 10 * time.Second

// hostRefsMu orders pool spec writes against RemoveHost. CreatePool and
// UpdatePool hold it for reading from their flintlock_hosts check through
// their store write; RemoveHost holds it for writing across
// store.DeleteHost, whose own transaction checks that no pool names the
// host. Without it, a pool could be stored naming a host that RemoveHost
// deleted after the pool's check passed. It is package-level because the
// PoolAdmin and HostAdmin servers are separate values that share one store
// in one process.
var hostRefsMu sync.RWMutex

// HostAdminServer implements poolmgrv1alpha1.HostAdminServer: the registry
// of flintlock hosts and their cordon state. It keeps flint's connections
// in step with the store's host rows.
type HostAdminServer struct {
	poolmgrv1alpha1.UnimplementedHostAdminServer

	store store.Store
	flint *flintlockclient.Pool

	hostLocksMu sync.Mutex
	hostLocks   map[string]*sync.Mutex
}

// NewHostAdminServer returns a HostAdminServer backed by st that adds,
// replaces, and removes hosts' connections in flint.
func NewHostAdminServer(st store.Store, flint *flintlockclient.Pool) *HostAdminServer {
	return &HostAdminServer{store: st, flint: flint, hostLocks: make(map[string]*sync.Mutex)}
}

// lockHost serializes AddHost/UpdateHost/RemoveHost for the same host name,
// for the same reason PoolAdminServer.lockPool does for pools: each RPC
// writes the store and then the client pool, and two interleaved calls
// could otherwise leave the pool holding a connection that disagrees with
// the store (or one for a host the store no longer has). Returns an unlock
// function.
func (s *HostAdminServer) lockHost(name string) func() {
	s.hostLocksMu.Lock()
	l, ok := s.hostLocks[name]
	if !ok {
		l = &sync.Mutex{}
		s.hostLocks[name] = l
	}
	s.hostLocksMu.Unlock()

	l.Lock()
	return l.Unlock
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
// across all pools, and the flintlock version it last reported. A cordoned
// host at 0 is safe to take down.
func (s *HostAdminServer) ListHosts(ctx context.Context, _ *poolmgrv1alpha1.ListHostsRequest) (*poolmgrv1alpha1.ListHostsResponse, error) {
	hosts, err := s.store.ListHosts(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list hosts: %v", err)
	}

	resp := &poolmgrv1alpha1.ListHostsResponse{}
	for _, h := range hosts {
		hs, err := s.hostStatus(ctx, h)
		if err != nil {
			return nil, err
		}
		resp.Hosts = append(resp.Hosts, hs)
	}
	return resp, nil
}

// GetHost returns the named host with its VM count and last reported
// flintlock version. It reads the store and the client pool only; it never
// probes the host.
func (s *HostAdminServer) GetHost(ctx context.Context, req *poolmgrv1alpha1.GetHostRequest) (*poolmgrv1alpha1.HostStatus, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	host, err := s.store.GetHost(ctx, req.GetName())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "host %q not found", req.GetName())
		}
		return nil, status.Errorf(codes.Internal, "get host: %v", err)
	}
	return s.hostStatus(ctx, host)
}

// hostStatus pairs host with its VM count and flintlock version.
func (s *HostAdminServer) hostStatus(ctx context.Context, host *poolmgrv1alpha1.Host) (*poolmgrv1alpha1.HostStatus, error) {
	count, err := s.store.CountVMsByHost(ctx, host.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "count vms for host %q: %v", host.GetName(), err)
	}
	version, _ := s.flint.FlintlockVersion(host.GetName())
	return &poolmgrv1alpha1.HostStatus{Host: host, VmCount: count, FlintlockVersion: version}, nil
}

// AddHost registers a new host and adds its connection to the client pool.
// A malformed spec fails with InvalidArgument; TLS files that can't be
// read, or (unless skip_validation is set) a host that is unreachable or
// runs a flintlock older than flintlockclient.MinFlintlockVersion, fail
// with FailedPrecondition; a duplicate name fails with AlreadyExists. On
// any failure nothing is stored. The new host starts uncordoned whatever
// the request's cordon fields say.
func (s *HostAdminServer) AddHost(ctx context.Context, req *poolmgrv1alpha1.AddHostRequest) (*poolmgrv1alpha1.Host, error) {
	host := &poolmgrv1alpha1.Host{
		Name:      req.GetHost().GetName(),
		Address:   req.GetHost().GetAddress(),
		Tls:       req.GetHost().GetTls(),
		UpdatedAt: timestamppb.Now(),
	}

	log := slog.Default().With("host", host.GetName())
	log.InfoContext(ctx, "hostadmin: AddHost requested", "address", host.GetAddress(), "skip_validation", req.GetSkipValidation())

	if host.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "host.name is required")
	}

	unlock := s.lockHost(host.GetName())
	defer unlock()

	_, err := s.store.GetHost(ctx, host.GetName())
	if err == nil {
		log.WarnContext(ctx, "hostadmin: AddHost failed: host already exists")
		return nil, status.Errorf(codes.AlreadyExists, "host %q already exists", host.GetName())
	}
	if !errors.Is(err, store.ErrNotFound) {
		log.ErrorContext(ctx, "hostadmin: AddHost: get host failed", "error", err)
		return nil, status.Errorf(codes.Internal, "get host: %v", err)
	}

	conn, err := s.dialHost(ctx, log, host, req.GetSkipValidation())
	if err != nil {
		log.WarnContext(ctx, "hostadmin: AddHost failed", "error", err)
		return nil, err
	}

	if err := s.store.CreateHost(ctx, host); err != nil {
		_ = conn.Close()
		if errors.Is(err, store.ErrHostExists) {
			log.WarnContext(ctx, "hostadmin: AddHost failed: host already exists")
			return nil, status.Errorf(codes.AlreadyExists, "host %q already exists", host.GetName())
		}
		log.ErrorContext(ctx, "hostadmin: AddHost: store write failed", "error", err)
		return nil, status.Errorf(codes.Internal, "create host: %v", err)
	}

	if err := s.install(ctx, log, conn); err != nil {
		_ = conn.Close()
		log.ErrorContext(ctx, "hostadmin: AddHost: add to client pool failed", "error", err)
		return nil, status.Errorf(codes.Internal, "add host %q to client pool: %v", host.GetName(), err)
	}

	log.InfoContext(ctx, "hostadmin: host added")
	return host, nil
}

// UpdateHost replaces the address and TLS settings of an existing host and
// swaps its connection in the client pool, closing the old one. It fails
// with NotFound for an unknown host, and otherwise validates like AddHost.
// It never renames a host and never touches its cordon state.
func (s *HostAdminServer) UpdateHost(ctx context.Context, req *poolmgrv1alpha1.UpdateHostRequest) (*poolmgrv1alpha1.Host, error) {
	host := &poolmgrv1alpha1.Host{
		Name:    req.GetHost().GetName(),
		Address: req.GetHost().GetAddress(),
		Tls:     req.GetHost().GetTls(),
	}

	log := slog.Default().With("host", host.GetName())
	log.InfoContext(ctx, "hostadmin: UpdateHost requested", "address", host.GetAddress(), "skip_validation", req.GetSkipValidation())

	if host.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "host.name is required")
	}

	unlock := s.lockHost(host.GetName())
	defer unlock()

	if _, err := s.store.GetHost(ctx, host.GetName()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			log.WarnContext(ctx, "hostadmin: UpdateHost failed: host not found")
			return nil, status.Errorf(codes.NotFound, "host %q not found", host.GetName())
		}
		log.ErrorContext(ctx, "hostadmin: UpdateHost: get host failed", "error", err)
		return nil, status.Errorf(codes.Internal, "get host: %v", err)
	}

	conn, err := s.dialHost(ctx, log, host, req.GetSkipValidation())
	if err != nil {
		log.WarnContext(ctx, "hostadmin: UpdateHost failed", "error", err)
		return nil, err
	}

	updated, err := s.store.UpdateHost(ctx, host)
	if err != nil {
		_ = conn.Close()
		if errors.Is(err, store.ErrNotFound) {
			log.WarnContext(ctx, "hostadmin: UpdateHost failed: host not found")
			return nil, status.Errorf(codes.NotFound, "host %q not found", host.GetName())
		}
		log.ErrorContext(ctx, "hostadmin: UpdateHost: store write failed", "error", err)
		return nil, status.Errorf(codes.Internal, "update host: %v", err)
	}

	if err := s.install(ctx, log, conn); err != nil {
		_ = conn.Close()
		log.ErrorContext(ctx, "hostadmin: UpdateHost: replace in client pool failed", "error", err)
		return nil, status.Errorf(codes.Internal, "replace host %q in client pool: %v", host.GetName(), err)
	}

	log.InfoContext(ctx, "hostadmin: host updated")
	return updated, nil
}

// RemoveHost unregisters the named host and closes its connection. It
// fails with NotFound for an unknown host, and with FailedPrecondition
// while any pool spec names the host in flintlock_hosts or any VM record or
// placement reservation is counted against it. store.DeleteHost makes that
// check and the delete in one transaction, and hostRefsMu keeps a
// CreatePool or UpdatePool from slipping a new reference in between.
//
// One narrow window remains. A reconciler that UpdatePool is replacing
// can still be running the old spec for a moment after the new one is
// stored, and PickHost may choose the removed host from it. If it does so
// after the delete commits (ReservePlacement treats an unregistered host as
// uncordoned) but before flint.Remove below, it could create a VM on the
// host. Closing it means making ReservePlacement refuse unregistered hosts,
// which many existing callers that place on hosts with no registry row
// (tests, mostly) don't yet allow.
func (s *HostAdminServer) RemoveHost(ctx context.Context, req *poolmgrv1alpha1.RemoveHostRequest) (*emptypb.Empty, error) {
	name := req.GetName()

	log := slog.Default().With("host", name)
	log.InfoContext(ctx, "hostadmin: RemoveHost requested")

	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	unlock := s.lockHost(name)
	defer unlock()

	hostRefsMu.Lock()
	err := s.store.DeleteHost(ctx, name)
	hostRefsMu.Unlock()

	var inUse *store.HostInUseError
	switch {
	case errors.Is(err, store.ErrNotFound):
		log.WarnContext(ctx, "hostadmin: RemoveHost failed: host not found")
		return nil, status.Errorf(codes.NotFound, "host %q not found", name)
	case errors.As(err, &inUse):
		log.WarnContext(ctx, "hostadmin: RemoveHost failed: host still in use", "pools", inUse.Pools, "vm_count", inUse.VMCount)
		return nil, status.Error(codes.FailedPrecondition, hostInUseMessage(name, inUse))
	case err != nil:
		log.ErrorContext(ctx, "hostadmin: RemoveHost: store delete failed", "error", err)
		return nil, status.Errorf(codes.Internal, "delete host: %v", err)
	}

	// A host skipped at startup (see cmd/poolmgrd) has no connection to
	// remove; any other error is a failure to close the old connection,
	// which the pool has already dropped.
	if err := s.flint.Remove(name); err != nil && !errors.Is(err, flintlockclient.ErrUnknownHost) {
		log.WarnContext(ctx, "hostadmin: RemoveHost: close connection failed", "error", err)
	}

	log.InfoContext(ctx, "hostadmin: host removed")
	return &emptypb.Empty{}, nil
}

// hostInUseMessage explains why RemoveHost refused to remove name.
func hostInUseMessage(name string, inUse *store.HostInUseError) string {
	var reasons []string
	if len(inUse.Pools) > 0 {
		reasons = append(reasons, fmt.Sprintf("named in flintlock_hosts by pools %s", strings.Join(inUse.Pools, ", ")))
	}
	if inUse.VMCount > 0 {
		reasons = append(reasons, fmt.Sprintf("vm_count is %d", inUse.VMCount))
	}
	return fmt.Sprintf("host %q is still in use (%s): cordon it, drop it from each pool spec, and wait for vm_count to reach 0",
		name, strings.Join(reasons, "; "))
}

// dialHost builds a connection for host and, unless skipValidation is set,
// checks the flintlock version it runs. The error is a gRPC status: a
// malformed spec is InvalidArgument, anything else FailedPrecondition. On
// success the caller owns the returned Conn.
func (s *HostAdminServer) dialHost(ctx context.Context, log *slog.Logger, host *poolmgrv1alpha1.Host, skipValidation bool) (*flintlockclient.Conn, error) {
	conn, err := flintlockclient.Dial(host)
	if errors.Is(err, flintlockclient.ErrInvalidHost) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if skipValidation {
		return conn, nil
	}

	checkCtx, cancel := context.WithTimeout(ctx, hostCheckTimeout)
	defer cancel()
	if err := conn.CheckVersion(checkCtx); err != nil {
		_ = conn.Close()
		return nil, status.Errorf(codes.FailedPrecondition, "validate host %q: %v", host.GetName(), err)
	}
	version, _ := conn.FlintlockVersion()
	log.DebugContext(ctx, "hostadmin: host validated", "flintlock_version", version)
	return conn, nil
}

// install puts conn in the client pool, replacing any connection already
// there for its host. The replace path covers AddHost finding a stale
// connection; the add path covers UpdateHost on a host whose stored spec
// failed to dial at startup and so never joined the pool. On error the
// caller still owns conn.
func (s *HostAdminServer) install(ctx context.Context, log *slog.Logger, conn *flintlockclient.Conn) error {
	err := s.flint.Update(conn)
	if errors.Is(err, flintlockclient.ErrUnknownHost) {
		return s.flint.Add(conn)
	}
	if err != nil {
		// conn is in the pool; only closing the old connection failed.
		log.WarnContext(ctx, "hostadmin: close old connection failed", "error", err)
	}
	return nil
}
