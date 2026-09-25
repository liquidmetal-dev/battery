package api_test

import (
	"context"
	"net"
	"strings"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func seedTestHost(ctx context.Context, t *testing.T, st store.Store, name string) {
	t.Helper()
	host := &poolmgrv1alpha1.Host{Name: name, Address: name + ":8443", UpdatedAt: timestamppb.Now()}
	if err := st.UpsertHostIfMissing(ctx, host); err != nil {
		t.Fatalf("UpsertHostIfMissing(%q) error = %v", name, err)
	}
}

func TestCordonAndUncordonHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")
	s := newHostAdmin(t, st)

	cordoned, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "host-a", Reason: "kernel upgrade"})
	if err != nil {
		t.Fatalf("CordonHost() error = %v", err)
	}
	if !cordoned.GetCordoned() || cordoned.GetCordonedReason() != "kernel upgrade" {
		t.Errorf("CordonHost() = %+v, want cordoned=true reason=%q", cordoned, "kernel upgrade")
	}

	uncordoned, err := s.UncordonHost(ctx, &poolmgrv1alpha1.UncordonHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("UncordonHost() error = %v", err)
	}
	if uncordoned.GetCordoned() {
		t.Errorf("UncordonHost() cordoned = true, want false")
	}
}

func TestCordonHostIdempotent(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")
	s := newHostAdmin(t, st)

	if _, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "host-a"}); err != nil {
		t.Fatalf("CordonHost() error = %v", err)
	}
	if _, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "host-a"}); err != nil {
		t.Fatalf("CordonHost() second call error = %v", err)
	}
}

func TestCordonHostUnknownHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := newHostAdmin(t, st)

	_, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "missing"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CordonHost() error = %v, want FailedPrecondition", err)
	}
}

func TestUncordonHostUnknownHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := newHostAdmin(t, st)

	_, err := s.UncordonHost(ctx, &poolmgrv1alpha1.UncordonHostRequest{Name: "missing"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("UncordonHost() error = %v, want FailedPrecondition", err)
	}
}

func TestCordonHostRequiresName(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := newHostAdmin(t, st)

	_, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CordonHost() error = %v, want InvalidArgument", err)
	}
}

func TestListHosts(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")
	seedTestHost(ctx, t, st, "host-b")
	s := newHostAdmin(t, st)

	if _, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "host-b"}); err != nil {
		t.Fatalf("CordonHost() error = %v", err)
	}

	resp, err := s.ListHosts(ctx, &poolmgrv1alpha1.ListHostsRequest{})
	if err != nil {
		t.Fatalf("ListHosts() error = %v", err)
	}
	if len(resp.GetHosts()) != 2 {
		t.Fatalf("ListHosts() = %d hosts, want 2", len(resp.GetHosts()))
	}
	if resp.GetHosts()[0].GetHost().GetName() != "host-a" || resp.GetHosts()[0].GetVmCount() != 0 {
		t.Errorf("ListHosts()[0] = %+v, want host-a with 0 VMs", resp.GetHosts()[0])
	}
	if !resp.GetHosts()[1].GetHost().GetCordoned() {
		t.Errorf("ListHosts()[1] = %+v, want host-b cordoned", resp.GetHosts()[1])
	}
}

// TestListHosts_VmCountIncludesDeletingAndReservations: vm_count is what an
// operator reads to decide when a cordoned host is empty, so it must include
// VMs whose deletion is still pending or that are quarantined (both still
// exist on the host) and placements that are reserved but not yet recorded.
func TestListHosts_VmCountIncludesDeletingAndReservations(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")

	deleting := sampleAvailableVM("vm-deleting", "pool-a")
	deleting.Phase = poolmgrv1alpha1.VMPhase_DELETING
	quarantined := sampleAvailableVM("vm-quarantined", "pool-a")
	quarantined.Phase = poolmgrv1alpha1.VMPhase_QUARANTINED
	for _, vm := range []*poolmgrv1alpha1.VMRecord{deleting, quarantined} {
		if err := st.CreateVM(ctx, vm); err != nil {
			t.Fatalf("CreateVM(%s) error = %v", vm.GetUid(), err)
		}
	}
	if err := st.ReservePlacement(ctx, "placement-1", "host-a", "pool-a", "default"); err != nil {
		t.Fatalf("ReservePlacement() error = %v", err)
	}

	resp, err := newHostAdmin(t, st).ListHosts(ctx, &poolmgrv1alpha1.ListHostsRequest{})
	if err != nil {
		t.Fatalf("ListHosts() error = %v", err)
	}
	if len(resp.GetHosts()) != 1 {
		t.Fatalf("ListHosts() = %d hosts, want 1", len(resp.GetHosts()))
	}
	if got := resp.GetHosts()[0].GetVmCount(); got != 3 {
		t.Errorf("ListHosts() host-a vm_count = %d, want 3 (DELETING + QUARANTINED + reservation)", got)
	}
}

// newHostAdmin returns a HostAdminServer backed by st and an empty client
// pool.
func newHostAdmin(t *testing.T, st store.Store) *api.HostAdminServer {
	t.Helper()
	s, _ := newHostAdminWithPool(t, st)
	return s
}

// newHostAdminWithPool is newHostAdmin, also returning the client pool so
// tests can check what the server put in it.
func newHostAdminWithPool(t *testing.T, st store.Store) (*api.HostAdminServer, *flintlockclient.Pool) {
	t.Helper()
	flint, err := flintlockclient.New(nil)
	if err != nil {
		t.Fatalf("flintlockclient.New() error = %v", err)
	}
	t.Cleanup(func() { _ = flint.Close() })
	return api.NewHostAdminServer(st, flint), flint
}

// versionedMicroVM is a fake flintlock MicroVM service whose ServerInfo
// reports version.
type versionedMicroVM struct {
	microvmv1alpha1.UnimplementedMicroVMServer

	version string
}

func (f *versionedMicroVM) ServerInfo(context.Context, *emptypb.Empty) (*microvmv1alpha1.ServerInfoResponse, error) {
	return &microvmv1alpha1.ServerInfoResponse{Version: &microvmv1alpha1.VersionInfo{Version: f.version}}, nil
}

// startFlintlockServer serves vm on a loopback listener and returns its
// address.
func startFlintlockServer(t *testing.T, vm microvmv1alpha1.MicroVMServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	microvmv1alpha1.RegisterMicroVMServer(srv, vm)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// startFlintlockVersion starts a fake flintlock reporting version and
// returns its address.
func startFlintlockVersion(t *testing.T, version string) string {
	t.Helper()
	return startFlintlockServer(t, &versionedMicroVM{version: version})
}

// closedAddr returns a loopback address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

// insecureHost returns a Host spec for name at address with TLS off.
func insecureHost(name, address string) *poolmgrv1alpha1.Host {
	return &poolmgrv1alpha1.Host{Name: name, Address: address, Tls: &poolmgrv1alpha1.HostTLS{Insecure: true}}
}

// assertNoHosts fails t if st or flint holds any host.
func assertNoHosts(ctx context.Context, t *testing.T, st store.Store, flint *flintlockclient.Pool) {
	t.Helper()
	hosts, err := st.ListHosts(ctx)
	if err != nil {
		t.Fatalf("ListHosts() error = %v", err)
	}
	if len(hosts) != 0 {
		t.Errorf("store hosts = %v, want none", hosts)
	}
	if names := flint.Hosts(); len(names) != 0 {
		t.Errorf("client pool hosts = %v, want none", names)
	}
}

func TestAddHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s, flint := newHostAdminWithPool(t, st)
	addr := startFlintlockVersion(t, "v0.15.2")

	req := insecureHost("host-a", addr)
	req.Cordoned = true
	req.CordonedReason = "ignored"
	got, err := s.AddHost(ctx, &poolmgrv1alpha1.AddHostRequest{Host: req})
	if err != nil {
		t.Fatalf("AddHost() error = %v", err)
	}
	if got.GetName() != "host-a" || got.GetAddress() != addr || !got.GetTls().GetInsecure() || got.GetCordoned() || got.GetCordonedReason() != "" {
		t.Errorf("AddHost() = %+v, want host-a at %s, insecure, uncordoned", got, addr)
	}

	stored, err := st.GetHost(ctx, "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if !proto.Equal(stored, got) {
		t.Errorf("stored host = %+v, want %+v", stored, got)
	}
	if poolAddr, err := flint.Address("host-a"); err != nil || poolAddr != addr {
		t.Errorf("flint.Address() = %q, %v, want %q", poolAddr, err, addr)
	}

	hs, err := s.GetHost(ctx, &poolmgrv1alpha1.GetHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if hs.GetFlintlockVersion() != "v0.15.2" || hs.GetVmCount() != 0 {
		t.Errorf("GetHost() = %+v, want flintlock_version v0.15.2 and vm_count 0", hs)
	}

	list, err := s.ListHosts(ctx, &poolmgrv1alpha1.ListHostsRequest{})
	if err != nil {
		t.Fatalf("ListHosts() error = %v", err)
	}
	if len(list.GetHosts()) != 1 || list.GetHosts()[0].GetFlintlockVersion() != "v0.15.2" {
		t.Errorf("ListHosts() = %+v, want host-a with flintlock_version v0.15.2", list.GetHosts())
	}
}

func TestAddHost_InvalidSpec(t *testing.T) {
	tests := []struct {
		name string
		host *poolmgrv1alpha1.Host
	}{
		{"no host", nil},
		{"no name", insecureHost("", "10.0.0.1:9090")},
		{"no address", insecureHost("host-a", "")},
		{"no tls", &poolmgrv1alpha1.Host{Name: "host-a", Address: "10.0.0.1:9090"}},
		{"insecure with ca_file", &poolmgrv1alpha1.Host{Name: "host-a", Address: "10.0.0.1:9090",
			Tls: &poolmgrv1alpha1.HostTLS{Insecure: true, CaFile: "ca.pem"}}},
		{"cert_file without key_file", &poolmgrv1alpha1.Host{Name: "host-a", Address: "10.0.0.1:9090",
			Tls: &poolmgrv1alpha1.HostTLS{CaFile: "ca.pem", CertFile: "cert.pem"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := openTestStore(t)
			s, flint := newHostAdminWithPool(t, st)

			// skip_validation doesn't skip the spec rules.
			_, err := s.AddHost(ctx, &poolmgrv1alpha1.AddHostRequest{Host: tt.host, SkipValidation: true})
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("AddHost() error = %v, want InvalidArgument", err)
			}
			assertNoHosts(ctx, t, st, flint)
		})
	}
}

func TestAddHost_ValidationFailures(t *testing.T) {
	tests := []struct {
		name string
		host func(t *testing.T) *poolmgrv1alpha1.Host
	}{
		{"unreachable", func(t *testing.T) *poolmgrv1alpha1.Host {
			return insecureHost("host-a", closedAddr(t))
		}},
		{"too old", func(t *testing.T) *poolmgrv1alpha1.Host {
			return insecureHost("host-a", startFlintlockVersion(t, "v0.15.1"))
		}},
		{"older than v0.15.0 (no ServerInfo)", func(t *testing.T) *poolmgrv1alpha1.Host {
			return insecureHost("host-a", startFlintlockServer(t, &fakeMicroVM{}))
		}},
		{"unreadable ca_file", func(*testing.T) *poolmgrv1alpha1.Host {
			return &poolmgrv1alpha1.Host{Name: "host-a", Address: "10.0.0.1:9090",
				Tls: &poolmgrv1alpha1.HostTLS{CaFile: "/nonexistent/ca.pem"}}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := openTestStore(t)
			s, flint := newHostAdminWithPool(t, st)

			_, err := s.AddHost(ctx, &poolmgrv1alpha1.AddHostRequest{Host: tt.host(t)})
			if status.Code(err) != codes.FailedPrecondition {
				t.Errorf("AddHost() error = %v, want FailedPrecondition", err)
			}
			assertNoHosts(ctx, t, st, flint)
		})
	}
}

func TestAddHost_SkipValidation(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s, flint := newHostAdminWithPool(t, st)
	addr := closedAddr(t)

	if _, err := s.AddHost(ctx, &poolmgrv1alpha1.AddHostRequest{Host: insecureHost("host-a", addr), SkipValidation: true}); err != nil {
		t.Fatalf("AddHost() error = %v", err)
	}
	if _, err := st.GetHost(ctx, "host-a"); err != nil {
		t.Errorf("GetHost() error = %v, want host-a stored", err)
	}
	if poolAddr, err := flint.Address("host-a"); err != nil || poolAddr != addr {
		t.Errorf("flint.Address() = %q, %v, want %q", poolAddr, err, addr)
	}

	hs, err := s.GetHost(ctx, &poolmgrv1alpha1.GetHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if hs.GetFlintlockVersion() != "" {
		t.Errorf("GetHost() flintlock_version = %q, want empty for an unchecked host", hs.GetFlintlockVersion())
	}
}

func TestAddHost_AlreadyExists(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s, flint := newHostAdminWithPool(t, st)

	first := insecureHost("host-a", "10.0.0.1:9090")
	if _, err := s.AddHost(ctx, &poolmgrv1alpha1.AddHostRequest{Host: first, SkipValidation: true}); err != nil {
		t.Fatalf("AddHost() error = %v", err)
	}
	_, err := s.AddHost(ctx, &poolmgrv1alpha1.AddHostRequest{Host: insecureHost("host-a", "10.0.0.2:9090"), SkipValidation: true})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("AddHost() second call error = %v, want AlreadyExists", err)
	}
	if poolAddr, _ := flint.Address("host-a"); poolAddr != "10.0.0.1:9090" {
		t.Errorf("flint.Address() = %q, want the first host's address kept", poolAddr)
	}
}

func TestUpdateHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s, flint := newHostAdminWithPool(t, st)

	oldAddr := startFlintlockVersion(t, "v0.15.2")
	if _, err := s.AddHost(ctx, &poolmgrv1alpha1.AddHostRequest{Host: insecureHost("host-a", oldAddr)}); err != nil {
		t.Fatalf("AddHost() error = %v", err)
	}
	if _, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "host-a", Reason: "moving"}); err != nil {
		t.Fatalf("CordonHost() error = %v", err)
	}

	newAddr := startFlintlockVersion(t, "v0.16.0")
	req := insecureHost("host-a", newAddr)
	req.Cordoned = false
	got, err := s.UpdateHost(ctx, &poolmgrv1alpha1.UpdateHostRequest{Host: req})
	if err != nil {
		t.Fatalf("UpdateHost() error = %v", err)
	}
	if got.GetAddress() != newAddr || !got.GetCordoned() || got.GetCordonedReason() != "moving" {
		t.Errorf("UpdateHost() = %+v, want address %s and cordon state kept", got, newAddr)
	}
	if poolAddr, err := flint.Address("host-a"); err != nil || poolAddr != newAddr {
		t.Errorf("flint.Address() = %q, %v, want %q", poolAddr, err, newAddr)
	}

	hs, err := s.GetHost(ctx, &poolmgrv1alpha1.GetHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if hs.GetFlintlockVersion() != "v0.16.0" {
		t.Errorf("GetHost() flintlock_version = %q, want the new connection's v0.16.0", hs.GetFlintlockVersion())
	}
}

// TestUpdateHost_NotFound: an unknown host is reported before the manager
// tries to dial the new address, which here would fail.
func TestUpdateHost_NotFound(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s, flint := newHostAdminWithPool(t, st)

	_, err := s.UpdateHost(ctx, &poolmgrv1alpha1.UpdateHostRequest{Host: insecureHost("missing", closedAddr(t))})
	if status.Code(err) != codes.NotFound {
		t.Errorf("UpdateHost() error = %v, want NotFound", err)
	}
	assertNoHosts(ctx, t, st, flint)
}

func TestUpdateHost_InvalidSpec(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := newHostAdmin(t, st)
	seedTestHost(ctx, t, st, "host-a")

	for _, host := range []*poolmgrv1alpha1.Host{
		insecureHost("", "10.0.0.1:9090"),
		insecureHost("host-a", ""),
	} {
		_, err := s.UpdateHost(ctx, &poolmgrv1alpha1.UpdateHostRequest{Host: host, SkipValidation: true})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("UpdateHost(%+v) error = %v, want InvalidArgument", host, err)
		}
	}
}

func TestUpdateHost_ValidationFailureKeepsHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s, flint := newHostAdminWithPool(t, st)

	oldAddr := startFlintlockVersion(t, "v0.15.2")
	if _, err := s.AddHost(ctx, &poolmgrv1alpha1.AddHostRequest{Host: insecureHost("host-a", oldAddr)}); err != nil {
		t.Fatalf("AddHost() error = %v", err)
	}

	_, err := s.UpdateHost(ctx, &poolmgrv1alpha1.UpdateHostRequest{Host: insecureHost("host-a", startFlintlockVersion(t, "v0.14.0"))})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UpdateHost() error = %v, want FailedPrecondition", err)
	}

	stored, err := st.GetHost(ctx, "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if stored.GetAddress() != oldAddr {
		t.Errorf("stored address = %q, want %q kept", stored.GetAddress(), oldAddr)
	}
	if poolAddr, _ := flint.Address("host-a"); poolAddr != oldAddr {
		t.Errorf("flint.Address() = %q, want %q kept", poolAddr, oldAddr)
	}
}

// TestUpdateHost_AddsHostMissingFromPool: a stored host that poolmgrd
// skipped at startup (its spec failed to dial) has no connection, and
// UpdateHost is how an operator fixes it, so it must add the connection.
func TestUpdateHost_AddsHostMissingFromPool(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s, flint := newHostAdminWithPool(t, st)

	broken := &poolmgrv1alpha1.Host{Name: "host-a", Address: "10.0.0.1:9090",
		Tls: &poolmgrv1alpha1.HostTLS{CaFile: "/nonexistent/ca.pem"}, UpdatedAt: timestamppb.Now()}
	if err := st.CreateHost(ctx, broken); err != nil {
		t.Fatalf("CreateHost() error = %v", err)
	}

	addr := startFlintlockVersion(t, "v0.15.2")
	if _, err := s.UpdateHost(ctx, &poolmgrv1alpha1.UpdateHostRequest{Host: insecureHost("host-a", addr)}); err != nil {
		t.Fatalf("UpdateHost() error = %v", err)
	}
	if poolAddr, err := flint.Address("host-a"); err != nil || poolAddr != addr {
		t.Errorf("flint.Address() = %q, %v, want %q", poolAddr, err, addr)
	}
}

func TestRemoveHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s, flint := newHostAdminWithPool(t, st)

	if _, err := s.AddHost(ctx, &poolmgrv1alpha1.AddHostRequest{Host: insecureHost("host-a", "10.0.0.1:9090"), SkipValidation: true}); err != nil {
		t.Fatalf("AddHost() error = %v", err)
	}
	if _, err := s.RemoveHost(ctx, &poolmgrv1alpha1.RemoveHostRequest{Name: "host-a"}); err != nil {
		t.Fatalf("RemoveHost() error = %v", err)
	}
	assertNoHosts(ctx, t, st, flint)

	_, err := s.RemoveHost(ctx, &poolmgrv1alpha1.RemoveHostRequest{Name: "host-a"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("RemoveHost() second call error = %v, want NotFound", err)
	}
}

// TestRemoveHost_NotInPool: a host skipped at startup has a store row but
// no connection; removing it must still succeed.
func TestRemoveHost_NotInPool(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s, flint := newHostAdminWithPool(t, st)
	seedTestHost(ctx, t, st, "host-a")

	if _, err := s.RemoveHost(ctx, &poolmgrv1alpha1.RemoveHostRequest{Name: "host-a"}); err != nil {
		t.Fatalf("RemoveHost() error = %v", err)
	}
	assertNoHosts(ctx, t, st, flint)
}

func TestRemoveHost_RequiresName(t *testing.T) {
	_, err := newHostAdmin(t, openTestStore(t)).RemoveHost(context.Background(), &poolmgrv1alpha1.RemoveHostRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("RemoveHost() error = %v, want InvalidArgument", err)
	}
}

func TestRemoveHost_RefusedWhileInUse(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(ctx context.Context, t *testing.T, st store.Store)
		wantMsg string
	}{
		{
			name: "named by a pool",
			setup: func(ctx context.Context, t *testing.T, st store.Store) {
				if err := st.CreatePool(ctx, samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)); err != nil {
					t.Fatalf("CreatePool() error = %v", err)
				}
			},
			wantMsg: "default/pool-a",
		},
		{
			name: "vm on host",
			setup: func(ctx context.Context, t *testing.T, st store.Store) {
				if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
					t.Fatalf("CreateVM() error = %v", err)
				}
			},
			wantMsg: "vm_count is 1",
		},
		{
			name: "placement reserved",
			setup: func(ctx context.Context, t *testing.T, st store.Store) {
				if err := st.ReservePlacement(ctx, "placement-1", "host-a", "pool-a", "default"); err != nil {
					t.Fatalf("ReservePlacement() error = %v", err)
				}
			},
			wantMsg: "vm_count is 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := openTestStore(t)
			s, flint := newHostAdminWithPool(t, st)
			if _, err := s.AddHost(ctx, &poolmgrv1alpha1.AddHostRequest{Host: insecureHost("host-a", "10.0.0.1:9090"), SkipValidation: true}); err != nil {
				t.Fatalf("AddHost() error = %v", err)
			}
			tt.setup(ctx, t, st)

			_, err := s.RemoveHost(ctx, &poolmgrv1alpha1.RemoveHostRequest{Name: "host-a"})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("RemoveHost() error = %v, want FailedPrecondition", err)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tt.wantMsg) {
				t.Errorf("RemoveHost() message = %q, want it to mention %q", msg, tt.wantMsg)
			}
			if _, err := st.GetHost(ctx, "host-a"); err != nil {
				t.Errorf("GetHost() error = %v, want host-a kept", err)
			}
			if _, err := flint.Address("host-a"); err != nil {
				t.Errorf("flint.Address() error = %v, want host-a kept", err)
			}
		})
	}
}

func TestGetHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := newHostAdmin(t, st)
	seedTestHost(ctx, t, st, "host-a")
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	hs, err := s.GetHost(ctx, &poolmgrv1alpha1.GetHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if hs.GetHost().GetName() != "host-a" || hs.GetVmCount() != 1 {
		t.Errorf("GetHost() = %+v, want host-a with vm_count 1", hs)
	}

	_, err = s.GetHost(ctx, &poolmgrv1alpha1.GetHostRequest{Name: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetHost(missing) error = %v, want NotFound", err)
	}
}
