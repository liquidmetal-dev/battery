package poolmgrctl

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// TestPoolCreate_InvalidSpecFile_NoDial proves --spec-file validation
// happens before any gRPC dial is attempted: --addr points at an address
// nothing is listening on, and the error returned is the file-not-found
// error, not a dial/connection error (which, given a real unreachable
// address, would look nothing like "read spec file").
func TestPoolCreate_InvalidSpecFile_NoDial(t *testing.T) {
	root := NewRootCmd()

	var stderr bytes.Buffer
	root.SetErr(&stderr)
	root.SetArgs([]string{
		"--addr", "127.0.0.1:1", // nothing listens here
		"pool", "create",
		"--spec-file", filepath.Join(t.TempDir(), "does-not-exist.json"),
	})

	done := make(chan error, 1)
	go func() { done <- root.Execute() }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if !strings.Contains(err.Error(), "read spec file") {
			t.Fatalf("error = %v, want it to mention reading the spec file (proving validation ran before any dial)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out - looks like it tried to dial/connect instead of failing fast on the missing spec file")
	}
}

// TestPoolCreate_MissingSpecFile_RequiredFlagError proves that omitting
// --spec-file entirely produces cobra's standard "required flag(s) not
// set" error, not the confusing "read spec file : open : no such file or
// directory" that would result from trying to load an empty path.
func TestPoolCreate_MissingSpecFile_RequiredFlagError(t *testing.T) {
	root := NewRootCmd()

	var stderr bytes.Buffer
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&stderr)
	root.SetArgs([]string{"--insecure", "pool", "create"})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), `required flag(s) "spec-file" not set`) {
		t.Errorf("error = %q, want it to mention the required spec-file flag", err.Error())
	}
}

// TestPoolUpdate_MissingSpecFile_RequiredFlagError mirrors
// TestPoolCreate_MissingSpecFile_RequiredFlagError for "pool update".
func TestPoolUpdate_MissingSpecFile_RequiredFlagError(t *testing.T) {
	root := NewRootCmd()

	var stderr bytes.Buffer
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&stderr)
	root.SetArgs([]string{"--insecure", "pool", "update"})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), `required flag(s) "spec-file" not set`) {
		t.Errorf("error = %q, want it to mention the required spec-file flag", err.Error())
	}
}

// TestPoolList_NoTLSFlags_ClearError proves that running a command with no
// --insecure and no --ca-file produces the clear "either --ca-file or
// --insecure must be set" error, rather than a confusing CA-file-read
// error, on a first run with no connection flags set at all.
func TestPoolList_NoTLSFlags_ClearError(t *testing.T) {
	root := NewRootCmd()

	var stderr bytes.Buffer
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&stderr)
	root.SetArgs([]string{"pool", "list"})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "either --ca-file or --insecure must be set") {
		t.Errorf("error = %q, want it to mention either --ca-file or --insecure must be set", err.Error())
	}
}

// bufconnPoolAdmin starts a real api.PoolAdminServer backed by a temp
// SQLite store, serves it over an in-memory bufconn listener, and returns
// a dialed *grpc.ClientConn to it.
func bufconnPoolAdmin(t *testing.T) *grpc.ClientConn {
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
	poolmgrv1alpha1.RegisterPoolAdminServer(srv, api.NewPoolAdminServer(st, nil))
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

	return conn
}

// withTestClients returns a context carrying an apiClients built from conn,
// as PersistentPreRunE would, so a pool subcommand's RunE can be exercised
// directly without going through root's real dialing.
func withTestClients(conn *grpc.ClientConn) context.Context {
	clients := &apiClients{
		conn:      conn,
		poolAdmin: poolmgrv1alpha1.NewPoolAdminClient(conn),
	}
	return context.WithValue(context.Background(), clientsKey{}, clients)
}

func writeSpecFile(t *testing.T, spec *poolmgrv1alpha1.PoolSpec) string {
	t.Helper()

	data, err := protojson.Marshal(spec)
	if err != nil {
		t.Fatalf("protojson.Marshal() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write spec file: %v", err)
	}
	return path
}

func testPoolSpec(name, namespace string) *poolmgrv1alpha1.PoolSpec {
	return &poolmgrv1alpha1.PoolSpec{
		Name:            name,
		Namespace:       namespace,
		Size:            1,
		FlintlockHosts:  []string{"host-a"},
		MicrovmTemplate: &flintlocktypes.MicroVMSpec{Vcpu: 1},
		ReplenishmentStrategy: &poolmgrv1alpha1.ReplenishmentStrategy{
			Type:    poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD,
			MinSize: int32Ptr(1),
		},
		HookFailurePolicy:        poolmgrv1alpha1.HookFailurePolicy_QUARANTINE,
		HeartbeatInterval:        durationpb.New(30 * time.Second),
		HeartbeatExpiryThreshold: durationpb.New(90 * time.Second),
	}
}

func int32Ptr(v int32) *int32 { return &v }

func TestPoolCreate_Bufconn(t *testing.T) {
	conn := bufconnPoolAdmin(t)
	ctx := withTestClients(conn)

	specPath := writeSpecFile(t, testPoolSpec("pool-a", "default"))

	cmd := newPoolCreateCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--spec-file", specPath})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if !strings.Contains(out.String(), "pool-a") {
		t.Errorf("output missing pool-a, got:\n%s", out.String())
	}
}

func TestPoolGet_Bufconn(t *testing.T) {
	conn := bufconnPoolAdmin(t)
	ctx := withTestClients(conn)

	specPath := writeSpecFile(t, testPoolSpec("pool-a", "default"))

	createCmd := newPoolCreateCmd()
	createCmd.SetOut(&bytes.Buffer{})
	createCmd.SetContext(ctx)
	createCmd.SetArgs([]string{"--spec-file", specPath})
	if err := createCmd.Execute(); err != nil {
		t.Fatalf("create Execute() error = %v", err)
	}

	getCmd := newPoolGetCmd()
	var out bytes.Buffer
	getCmd.SetOut(&out)
	getCmd.SetContext(ctx)
	getCmd.SetArgs([]string{"--name", "pool-a", "--namespace", "default", "-o", "json"})

	if err := getCmd.Execute(); err != nil {
		t.Fatalf("get Execute() error = %v", err)
	}

	got := &poolmgrv1alpha1.Pool{}
	if err := protojson.Unmarshal(out.Bytes(), got); err != nil {
		t.Fatalf("protojson.Unmarshal() error = %v, output:\n%s", err, out.String())
	}
	if got.GetSpec().GetName() != "pool-a" || got.GetSpec().GetNamespace() != "default" {
		t.Errorf("got pool spec = %+v, want name=pool-a namespace=default", got.GetSpec())
	}
}

func TestPoolList_Bufconn(t *testing.T) {
	conn := bufconnPoolAdmin(t)
	ctx := withTestClients(conn)

	for _, spec := range []*poolmgrv1alpha1.PoolSpec{
		testPoolSpec("pool-a", "default"),
		testPoolSpec("pool-b", "default"),
	} {
		specPath := writeSpecFile(t, spec)
		createCmd := newPoolCreateCmd()
		createCmd.SetOut(&bytes.Buffer{})
		createCmd.SetContext(ctx)
		createCmd.SetArgs([]string{"--spec-file", specPath})
		if err := createCmd.Execute(); err != nil {
			t.Fatalf("create Execute() error = %v", err)
		}
	}

	listCmd := newPoolListCmd()
	var out bytes.Buffer
	listCmd.SetOut(&out)
	listCmd.SetContext(ctx)
	listCmd.SetArgs([]string{"-o", "json"})

	if err := listCmd.Execute(); err != nil {
		t.Fatalf("list Execute() error = %v", err)
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &raw); err != nil {
		t.Fatalf("json.Unmarshal() error = %v, output:\n%s", err, out.String())
	}
	if len(raw) != 2 {
		t.Fatalf("got %d pools, want 2", len(raw))
	}
}

func TestPoolDelete_Bufconn(t *testing.T) {
	conn := bufconnPoolAdmin(t)
	ctx := withTestClients(conn)

	specPath := writeSpecFile(t, testPoolSpec("pool-a", "default"))
	createCmd := newPoolCreateCmd()
	createCmd.SetOut(&bytes.Buffer{})
	createCmd.SetContext(ctx)
	createCmd.SetArgs([]string{"--spec-file", specPath})
	if err := createCmd.Execute(); err != nil {
		t.Fatalf("create Execute() error = %v", err)
	}

	deleteCmd := newPoolDeleteCmd()
	var out bytes.Buffer
	deleteCmd.SetOut(&out)
	deleteCmd.SetContext(ctx)
	deleteCmd.SetArgs([]string{"--name", "pool-a", "--namespace", "default"})

	if err := deleteCmd.Execute(); err != nil {
		t.Fatalf("delete Execute() error = %v", err)
	}
	if !strings.Contains(out.String(), "default/pool-a deleted") {
		t.Errorf("output = %q, want confirmation mentioning default/pool-a deleted", out.String())
	}

	getCmd := newPoolGetCmd()
	getCmd.SetOut(&bytes.Buffer{})
	getCmd.SetContext(ctx)
	getCmd.SetArgs([]string{"--name", "pool-a", "--namespace", "default"})
	if err := getCmd.Execute(); err == nil {
		t.Fatal("expected get after delete to fail, got nil")
	}
}
