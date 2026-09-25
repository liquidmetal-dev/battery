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
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
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
	flint, err := flintlockclient.New(nil)
	if err != nil {
		t.Fatalf("flintlockclient.New() error = %v", err)
	}
	t.Cleanup(func() { _ = flint.Close() })
	poolmgrv1alpha1.RegisterHostAdminServer(srv, api.NewHostAdminServer(st, flint))
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

func TestHostCordon_Bufconn(t *testing.T) {
	conn, st := bufconnHostAdmin(t)
	ctx := withTestHostAdminClient(conn)
	seedHost(t, st, "host-a")

	cmd := newHostCordonCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"host-a", "--reason", "kernel upgrade"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(out.String(), "host-a") || !strings.Contains(out.String(), "true") {
		t.Errorf("output = %q, want it to show host-a cordoned=true", out.String())
	}

	got, err := st.GetHost(context.Background(), "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if !got.GetCordoned() {
		t.Errorf("GetHost() cordoned = false, want true")
	}
}

func TestHostCordon_UnknownHost(t *testing.T) {
	conn, _ := bufconnHostAdmin(t)
	ctx := withTestHostAdminClient(conn)

	cmd := newHostCordonCmd()
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

func TestHostUncordon_Bufconn(t *testing.T) {
	conn, st := bufconnHostAdmin(t)
	ctx := withTestHostAdminClient(conn)
	seedHost(t, st, "host-a")
	if _, err := st.SetHostCordoned(context.Background(), "host-a", true, ""); err != nil {
		t.Fatalf("SetHostCordoned() error = %v", err)
	}

	cmd := newHostUncordonCmd()
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
	if got.GetCordoned() {
		t.Errorf("GetHost() cordoned = true, want false")
	}
}

func TestHostList_Bufconn(t *testing.T) {
	conn, st := bufconnHostAdmin(t)
	ctx := withTestHostAdminClient(conn)
	seedHost(t, st, "host-a")
	seedHost(t, st, "host-b")
	if _, err := st.SetHostCordoned(context.Background(), "host-b", true, ""); err != nil {
		t.Fatalf("SetHostCordoned() error = %v", err)
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

// runHostCmd executes cmd against conn with args and returns its output.
func runHostCmd(t *testing.T, conn *grpc.ClientConn, cmd *cobra.Command, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetContext(withTestHostAdminClient(conn))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
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

func TestHostAdd_Bufconn(t *testing.T) {
	conn, st := bufconnHostAdmin(t)

	out, err := runHostCmd(t, conn, newHostAddCmd(),
		"host-a", "--address", "10.0.0.1:9090", "--flintlock-insecure", "--skip-validation", "-o", "json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	got := &poolmgrv1alpha1.Host{}
	if err := protojson.Unmarshal([]byte(out), got); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, out)
	}
	if got.GetName() != "host-a" || got.GetAddress() != "10.0.0.1:9090" || !got.GetTls().GetInsecure() {
		t.Errorf("output = %+v, want host-a at 10.0.0.1:9090, insecure", got)
	}

	stored, err := st.GetHost(context.Background(), "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if stored.GetAddress() != "10.0.0.1:9090" || !stored.GetTls().GetInsecure() {
		t.Errorf("stored host = %+v, want address 10.0.0.1:9090, insecure", stored)
	}
}

func TestHostAdd_TLSFlags(t *testing.T) {
	conn, st := bufconnHostAdmin(t)

	// Paths that don't exist on the poolmgrd machine make the server refuse
	// the add even with --skip-validation, which proves the flags reached it.
	_, err := runHostCmd(t, conn, newHostAddCmd(),
		"host-a", "--address", "10.0.0.1:9090", "--flintlock-ca-file", "/nonexistent/ca.pem",
		"--flintlock-cert-file", "/nonexistent/cert.pem", "--flintlock-key-file", "/nonexistent/key.pem", "--skip-validation")
	if err == nil || !strings.Contains(err.Error(), "failed_precondition") || !strings.Contains(err.Error(), "read ca file") {
		t.Fatalf("Execute() error = %v, want failed_precondition about the CA file", err)
	}
	if _, err := st.GetHost(context.Background(), "host-a"); err == nil {
		t.Error("GetHost() found host-a, want nothing stored")
	}
}

func TestHostAdd_InvalidSpec(t *testing.T) {
	conn, _ := bufconnHostAdmin(t)

	_, err := runHostCmd(t, conn, newHostAddCmd(), "host-a", "--address", "10.0.0.1:9090", "--skip-validation")
	if err == nil || !strings.Contains(err.Error(), "invalid_argument") {
		t.Fatalf("Execute() error = %v, want invalid_argument (no TLS settings)", err)
	}
}

func TestHostAdd_RequiresAddress(t *testing.T) {
	conn, _ := bufconnHostAdmin(t)

	if _, err := runHostCmd(t, conn, newHostAddCmd(), "host-a", "--flintlock-insecure"); err == nil {
		t.Fatal("Execute() error = nil, want an error for missing --address")
	}
}

func TestHostAdd_ValidationFailure(t *testing.T) {
	conn, st := bufconnHostAdmin(t)

	_, err := runHostCmd(t, conn, newHostAddCmd(), "host-a", "--address", closedAddr(t), "--flintlock-insecure")
	if err == nil || !strings.Contains(err.Error(), "failed_precondition") {
		t.Fatalf("Execute() error = %v, want failed_precondition for an unreachable host", err)
	}
	if _, err := st.GetHost(context.Background(), "host-a"); err == nil {
		t.Error("GetHost() found host-a, want nothing stored")
	}
}

func TestHostUpdate_Bufconn(t *testing.T) {
	conn, st := bufconnHostAdmin(t)
	seedHost(t, st, "host-a")
	if _, err := st.SetHostCordoned(context.Background(), "host-a", true, "maintenance"); err != nil {
		t.Fatalf("SetHostCordoned() error = %v", err)
	}

	out, err := runHostCmd(t, conn, newHostUpdateCmd(), "host-a", "--address", "10.0.0.2:9090", "--flintlock-insecure", "--skip-validation")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(out, "10.0.0.2:9090") {
		t.Errorf("output = %q, want the new address", out)
	}

	got, err := st.GetHost(context.Background(), "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if got.GetAddress() != "10.0.0.2:9090" || !got.GetCordoned() {
		t.Errorf("GetHost() = %+v, want address 10.0.0.2:9090 and still cordoned", got)
	}
}

func TestHostUpdate_UnknownHost(t *testing.T) {
	conn, _ := bufconnHostAdmin(t)

	_, err := runHostCmd(t, conn, newHostUpdateCmd(), "missing", "--address", "10.0.0.2:9090", "--flintlock-insecure", "--skip-validation")
	if err == nil || !strings.Contains(err.Error(), "not_found") {
		t.Fatalf("Execute() error = %v, want not_found", err)
	}
}

func TestHostRemove_Bufconn(t *testing.T) {
	for _, tt := range []struct {
		format string
		want   string
	}{
		{"table", "host host-a removed"},
		{"json", `{"name":"host-a","removed":true}`},
	} {
		t.Run(tt.format, func(t *testing.T) {
			conn, st := bufconnHostAdmin(t)
			seedHost(t, st, "host-a")

			out, err := runHostCmd(t, conn, newHostRemoveCmd(), "host-a", "-o", tt.format)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if strings.TrimSpace(out) != tt.want {
				t.Errorf("output = %q, want %q", out, tt.want)
			}
			if _, err := st.GetHost(context.Background(), "host-a"); err == nil {
				t.Error("GetHost() found host-a after remove")
			}
		})
	}
}

func TestHostRemove_InUse(t *testing.T) {
	conn, st := bufconnHostAdmin(t)
	seedHost(t, st, "host-a")
	if err := st.ReservePlacement(context.Background(), "placement-1", "host-a", "pool-a", "default"); err != nil {
		t.Fatalf("ReservePlacement() error = %v", err)
	}

	_, err := runHostCmd(t, conn, newHostRemoveCmd(), "host-a")
	if err == nil || !strings.Contains(err.Error(), "failed_precondition") || !strings.Contains(err.Error(), "vm_count is 1") {
		t.Fatalf("Execute() error = %v, want failed_precondition naming vm_count", err)
	}
}

func TestHostGet_Bufconn(t *testing.T) {
	conn, st := bufconnHostAdmin(t)
	seedHost(t, st, "host-a")
	if err := st.ReservePlacement(context.Background(), "placement-1", "host-a", "pool-a", "default"); err != nil {
		t.Fatalf("ReservePlacement() error = %v", err)
	}

	out, err := runHostCmd(t, conn, newHostGetCmd(), "host-a")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for _, want := range []string{"VMS", "VERSION", "host-a", "1"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q, got:\n%s", want, out)
		}
	}

	out, err = runHostCmd(t, conn, newHostGetCmd(), "host-a", "-o", "json")
	if err != nil {
		t.Fatalf("Execute() -o json error = %v", err)
	}
	got := &poolmgrv1alpha1.HostStatus{}
	if err := protojson.Unmarshal([]byte(out), got); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, out)
	}
	if got.GetHost().GetName() != "host-a" || got.GetVmCount() != 1 {
		t.Errorf("output = %+v, want host-a with vm_count 1", got)
	}
}

func TestHostGet_UnknownHost(t *testing.T) {
	conn, _ := bufconnHostAdmin(t)

	_, err := runHostCmd(t, conn, newHostGetCmd(), "missing")
	if err == nil || !strings.Contains(err.Error(), "not_found") {
		t.Fatalf("Execute() error = %v, want not_found", err)
	}
}
