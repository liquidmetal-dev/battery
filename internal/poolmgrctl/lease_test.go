package poolmgrctl

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// bufconnLease starts a real api.LeaseServer backed by a temp SQLite store,
// serves it over an in-memory bufconn listener, and returns both the dialed
// *grpc.ClientConn and the store so tests can seed fixtures (pools, VMs,
// leases) directly. flint is nil - none of the paths exercised here
// (ClaimVM's resource_exhausted case, ListLeases, and ReleaseVM's
// VM-already-gone idempotent path) touch the flintlock client.
func bufconnLease(t *testing.T) (*grpc.ClientConn, store.Store) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "poolmgr.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	poolmgrv1alpha1.RegisterLeaseServer(srv, api.NewLeaseServer(st, nil, api.HookExecConfig{}, nil, nil))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn, st
}

// withTestLeaseClient returns a context carrying an apiClients with just
// the lease client set, as PersistentPreRunE would after dialing, so a
// lease subcommand's RunE can be exercised directly.
func withTestLeaseClient(conn *grpc.ClientConn) context.Context {
	clients := &apiClients{
		conn:  conn,
		lease: poolmgrv1alpha1.NewLeaseClient(conn),
	}
	return context.WithValue(context.Background(), clientsKey{}, clients)
}

func TestLeaseClaim_ResourceExhausted(t *testing.T) {
	conn, st := bufconnLease(t)
	ctx := withTestLeaseClient(conn)

	// A pool with no VMs at all: ClaimAvailableVM has nothing to hand out,
	// so ClaimVM must fail with RESOURCE_EXHAUSTED.
	if err := st.CreatePool(context.Background(), testPoolSpec("pool-a", "default")); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	cmd := newLeaseClaimCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--pool", "pool-a", "--namespace", "default"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "resource_exhausted") {
		t.Errorf("error = %q, want it to mention resource_exhausted", err.Error())
	}
	if !strings.Contains(err.Error(), "no available vm") {
		t.Errorf("error = %q, want a readable message about no available vm", err.Error())
	}
}

func TestLeaseRelease_Bufconn(t *testing.T) {
	conn, st := bufconnLease(t)
	ctx := withTestLeaseClient(conn)

	now := time.Now()
	lease := &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         "lease-1",
		VmUid:           "vm-1",
		PoolName:        "pool-a",
		PoolNamespace:   "default",
		ClaimedAt:       timestamppb.New(now),
		LastHeartbeatAt: timestamppb.New(now),
		ExpiresAt:       timestamppb.New(now.Add(time.Hour)),
	}
	// Deliberately no corresponding VM record: ReleaseVM's "VM record
	// already gone" idempotent path deletes the lease row without touching
	// the flintlock client, which this test's server has none of.
	if err := st.CreateLease(context.Background(), lease); err != nil {
		t.Fatalf("CreateLease: %v", err)
	}

	cmd := newLeaseReleaseCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--lease-id", "lease-1"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(out.String(), "lease lease-1 released") {
		t.Errorf("output = %q, want confirmation mentioning lease lease-1 released", out.String())
	}

	if _, err := st.GetLease(context.Background(), "lease-1"); err == nil {
		t.Error("expected lease to be gone after release, GetLease returned nil error")
	}
}

func TestLeaseRelease_NotFound(t *testing.T) {
	conn, _ := bufconnLease(t)
	ctx := withTestLeaseClient(conn)

	cmd := newLeaseReleaseCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--lease-id", "does-not-exist"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "not_found") {
		t.Errorf("error = %q, want it to mention not_found", err.Error())
	}
}

func seedLease(t *testing.T, st store.Store, leaseID, pool, namespace, vmUID string) {
	t.Helper()
	now := time.Now()
	lease := &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         leaseID,
		VmUid:           vmUID,
		PoolName:        pool,
		PoolNamespace:   namespace,
		ClaimedAt:       timestamppb.New(now),
		LastHeartbeatAt: timestamppb.New(now),
		ExpiresAt:       timestamppb.New(now.Add(time.Hour)),
	}
	if err := st.CreateLease(context.Background(), lease); err != nil {
		t.Fatalf("CreateLease: %v", err)
	}
}

func TestLeaseList_Unfiltered(t *testing.T) {
	conn, st := bufconnLease(t)
	ctx := withTestLeaseClient(conn)

	seedLease(t, st, "lease-1", "pool-a", "default", "vm-1")
	seedLease(t, st, "lease-2", "pool-b", "other", "vm-2")

	cmd := newLeaseListCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"-o", "json"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	for _, want := range []string{"lease-1", "lease-2"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q, got:\n%s", want, out.String())
		}
	}
}

func TestLeaseList_FilteredByPool(t *testing.T) {
	conn, st := bufconnLease(t)
	ctx := withTestLeaseClient(conn)

	seedLease(t, st, "lease-1", "pool-a", "default", "vm-1")
	seedLease(t, st, "lease-2", "pool-b", "other", "vm-2")

	cmd := newLeaseListCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--pool", "pool-a", "--namespace", "default", "-o", "json"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if !strings.Contains(out.String(), "lease-1") {
		t.Errorf("output missing lease-1, got:\n%s", out.String())
	}
	if strings.Contains(out.String(), "lease-2") {
		t.Errorf("output should not contain lease-2 when filtered to pool-a, got:\n%s", out.String())
	}
}

func TestLeaseList_PoolWithoutNamespace_ValidationError(t *testing.T) {
	cmd := newLeaseListCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetContext(withTestLeaseClient(nil))
	cmd.SetArgs([]string{"--pool", "pool-a"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected a validation error, got nil")
	}
	if !strings.Contains(err.Error(), "--pool and --namespace must be given together") {
		t.Errorf("error = %q, want it to mention --pool/--namespace must be given together", err.Error())
	}
}

func TestLeaseList_NamespaceWithoutPool_ValidationError(t *testing.T) {
	cmd := newLeaseListCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetContext(withTestLeaseClient(nil))
	cmd.SetArgs([]string{"--namespace", "default"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected a validation error, got nil")
	}
	if !strings.Contains(err.Error(), "--pool and --namespace must be given together") {
		t.Errorf("error = %q, want it to mention --pool/--namespace must be given together", err.Error())
	}
}
