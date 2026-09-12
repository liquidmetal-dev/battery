package poolmgrctl

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
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

// bufconnEvents starts a real api.EventsServer backed by a temp SQLite
// store, serves it over an in-memory bufconn listener, and returns both the
// dialed *grpc.ClientConn and the store so tests can seed events directly
// via AppendEvent before subscribing (Subscribe replays everything present
// in the outbox on connect).
func bufconnEvents(t *testing.T) (*grpc.ClientConn, store.Store) {
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
	// A short poll interval keeps the "cancelled context" test fast without
	// needing any new events to arrive.
	poolmgrv1alpha1.RegisterEventsServer(srv, api.NewEventsServer(st, 10*time.Millisecond, 0))
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

func withTestEventsClient(conn *grpc.ClientConn) context.Context {
	clients := &apiClients{
		conn:   conn,
		events: poolmgrv1alpha1.NewEventsClient(conn),
	}
	return context.WithValue(context.Background(), clientsKey{}, clients)
}

func seedEvent(t *testing.T, st store.Store, pool, namespace, vmUID string, eventType poolmgrv1alpha1.EventType) {
	t.Helper()
	e := &poolmgrv1alpha1.Event{
		PoolName:      pool,
		PoolNamespace: namespace,
		VmUid:         vmUID,
		Type:          eventType,
		CreatedAt:     timestamppb.Now(),
	}
	if err := st.AppendEvent(context.Background(), e); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
}

// syncBuffer wraps a bytes.Buffer with a mutex so it can be written by the
// command's goroutine and read by the test's goroutine concurrently without
// racing.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestEventsTail_Bufconn(t *testing.T) {
	conn, st := bufconnEvents(t)

	seedEvent(t, st, "pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	seedEvent(t, st, "pool-a", "default", "vm-2", poolmgrv1alpha1.EventType_VM_CLAIMED)

	ctx, cancel := context.WithTimeout(withTestEventsClient(conn), 5*time.Second)
	defer cancel()

	cmd := newEventsTailCmd()
	out := &syncBuffer{}
	cmd.SetOut(out)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--pool", "pool-a", "--namespace", "default"})

	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()

	// Give the two seeded events time to be replayed, then cancel so the
	// stream (which otherwise runs forever) stops.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Count(out.String(), "\n") >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute() error = %v, want nil on a cancelled context", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for events tail to exit after context cancel")
	}

	got := out.String()
	if !strings.Contains(got, "VM_PROVISIONED") || !strings.Contains(got, "vm-1") {
		t.Errorf("output missing first event, got:\n%s", got)
	}
	if !strings.Contains(got, "VM_CLAIMED") || !strings.Contains(got, "vm-2") {
		t.Errorf("output missing second event, got:\n%s", got)
	}
}

// fakeRecvResult is one queued return value for fakeEventClientStream.Recv.
type fakeRecvResult struct {
	event *poolmgrv1alpha1.Event
	err   error
}

// fakeEventClientStream is a minimal eventStream returning a fixed,
// strictly-ordered sequence of Recv results, used to test tailEvents'
// loop/exit logic without a real gRPC connection.
type fakeEventClientStream struct {
	results []fakeRecvResult
	i       int
}

func (f *fakeEventClientStream) Recv() (*poolmgrv1alpha1.Event, error) {
	r := f.results[f.i]
	f.i++
	return r.event, r.err
}

func TestTailEvents_EOFEndsCleanly(t *testing.T) {
	stream := &fakeEventClientStream{results: []fakeRecvResult{
		{event: &poolmgrv1alpha1.Event{VmUid: "vm-1", Type: poolmgrv1alpha1.EventType_VM_CLAIMED, CreatedAt: timestamppb.Now()}},
		{err: io.EOF},
	}}

	var out bytes.Buffer
	if err := tailEvents(context.Background(), &out, stream); err != nil {
		t.Fatalf("tailEvents() error = %v, want nil on io.EOF", err)
	}
	if !strings.Contains(out.String(), "vm-1") {
		t.Errorf("output missing event, got:\n%s", out.String())
	}
}

func TestTailEvents_CancelledContextEndsCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := &fakeEventClientStream{results: []fakeRecvResult{{err: context.Canceled}}}

	var out bytes.Buffer
	if err := tailEvents(ctx, &out, stream); err != nil {
		t.Fatalf("tailEvents() error = %v, want nil (a cancelled context is a normal exit, not an error)", err)
	}
}

func TestTailEvents_StreamErrorWraps(t *testing.T) {
	stream := &fakeEventClientStream{results: []fakeRecvResult{{err: errors.New("boom")}}}

	var out bytes.Buffer
	err := tailEvents(context.Background(), &out, stream)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error = %q, want it to mention the underlying stream error", err.Error())
	}
}
