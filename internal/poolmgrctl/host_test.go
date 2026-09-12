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

// bufconnHostAdmin starts a real api.HostAdminServer backed by a temp
// SQLite store, serves it over an in-memory bufconn listener, and returns
// both the dialed *grpc.ClientConn and the store so tests can seed fixtures
// (hosts) directly.
func bufconnHostAdmin(t *testing.T) (*grpc.ClientConn, store.Store) {
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
	poolmgrv1alpha1.RegisterHostAdminServer(srv, api.NewHostAdminServer(st))
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

// withTestHostAdminClient returns a context carrying an apiClients with
// just the hostAdmin client set, as PersistentPreRunE would after dialing,
// so a host subcommand's RunE can be exercised directly.
func withTestHostAdminClient(conn *grpc.ClientConn) context.Context {
	clients := &apiClients{
		conn:      conn,
		hostAdmin: poolmgrv1alpha1.NewHostAdminClient(conn),
	}
	return context.WithValue(context.Background(), clientsKey{}, clients)
}

func seedHost(t *testing.T, st store.Store, name string) {
	t.Helper()
	host := &poolmgrv1alpha1.Host{Name: name, Address: name + ":8443", UpdatedAt: timestamppb.New(time.Now())}
	if err := st.UpsertHostIfMissing(context.Background(), host); err != nil {
		t.Fatalf("UpsertHostIfMissing(%q): %v", name, err)
	}
}

func TestHostDrain_Bufconn(t *testing.T) {
	conn, st := bufconnHostAdmin(t)
	ctx := withTestHostAdminClient(conn)
	seedHost(t, st, "host-a")

	cmd := newHostDrainCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"host-a", "--reason", "kernel upgrade"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(out.String(), "host-a") || !strings.Contains(out.String(), "true") {
		t.Errorf("output = %q, want it to show host-a drained=true", out.String())
	}

	got, err := st.GetHost(context.Background(), "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if !got.GetDrained() {
		t.Errorf("GetHost() drained = false, want true")
	}
}

func TestHostDrain_UnknownHost(t *testing.T) {
	conn, _ := bufconnHostAdmin(t)
	ctx := withTestHostAdminClient(conn)

	cmd := newHostDrainCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"missing"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "failed_precondition") {
		t.Errorf("error = %q, want it to mention failed_precondition", err.Error())
	}
}

func TestHostUndrain_Bufconn(t *testing.T) {
	conn, st := bufconnHostAdmin(t)
	ctx := withTestHostAdminClient(conn)
	seedHost(t, st, "host-a")
	if _, err := st.SetHostDrained(context.Background(), "host-a", true, ""); err != nil {
		t.Fatalf("SetHostDrained() error = %v", err)
	}

	cmd := newHostUndrainCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"host-a"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	got, err := st.GetHost(context.Background(), "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if got.GetDrained() {
		t.Errorf("GetHost() drained = true, want false")
	}
}

func TestHostList_Bufconn(t *testing.T) {
	conn, st := bufconnHostAdmin(t)
	ctx := withTestHostAdminClient(conn)
	seedHost(t, st, "host-a")
	seedHost(t, st, "host-b")
	if _, err := st.SetHostDrained(context.Background(), "host-b", true, ""); err != nil {
		t.Fatalf("SetHostDrained() error = %v", err)
	}

	cmd := newHostListCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"-o", "json"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	for _, want := range []string{"host-a", "host-b"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q, got:\n%s", want, out.String())
		}
	}
}
