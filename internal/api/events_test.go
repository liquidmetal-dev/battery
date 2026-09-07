package api_test

import (
	"context"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc"

	"github.com/liquidmetal-dev/battery/internal/api"
)

// fakeEventStream implements grpc.ServerStreamingServer[Event] for testing
// EventsServer.Subscribe without a real network connection: Send delivers to
// a channel the test can read from, and Context is whatever the test wants
// to use to signal cancellation.
type fakeEventStream struct {
	grpc.ServerStream

	ctx  context.Context
	sent chan *poolmgrv1alpha1.Event
}

func newFakeEventStream(ctx context.Context) *fakeEventStream {
	return &fakeEventStream{ctx: ctx, sent: make(chan *poolmgrv1alpha1.Event, 16)}
}

func (f *fakeEventStream) Context() context.Context { return f.ctx }

func (f *fakeEventStream) Send(e *poolmgrv1alpha1.Event) error {
	f.sent <- e
	return nil
}

// recvEvent reads one event from stream.sent, failing the test if none
// arrives within a couple of seconds.
func recvEvent(t *testing.T, stream *fakeEventStream) *poolmgrv1alpha1.Event {
	t.Helper()
	select {
	case e := <-stream.sent:
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return nil
	}
}

// assertNoEvent fails the test if an event arrives on stream.sent within a
// short window.
func assertNoEvent(t *testing.T, stream *fakeEventStream) {
	t.Helper()
	select {
	case e := <-stream.sent:
		t.Fatalf("received unexpected event: %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSubscribe_ReplayOnConnect(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	e1 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	if err := st.AppendEvent(ctx, e1); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	e2 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_AVAILABLE)
	if err := st.AppendEvent(ctx, e2); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := newFakeEventStream(streamCtx)

	s := api.NewEventsServer(st, 10*time.Millisecond, 0)
	done := make(chan error, 1)
	go func() {
		done <- s.Subscribe(&poolmgrv1alpha1.SubscribeRequest{
			Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"},
		}, stream)
	}()

	got1 := recvEvent(t, stream)
	if got1.GetId() != e1.GetId() {
		t.Fatalf("first replayed event id = %d, want %d", got1.GetId(), e1.GetId())
	}
	got2 := recvEvent(t, stream)
	if got2.GetId() != e2.GetId() {
		t.Fatalf("second replayed event id = %d, want %d", got2.GetId(), e2.GetId())
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil after cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe() did not return after context cancellation")
	}
}

func TestSubscribe_LiveTailing(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := newFakeEventStream(streamCtx)

	s := api.NewEventsServer(st, 10*time.Millisecond, 0)
	done := make(chan error, 1)
	go func() {
		done <- s.Subscribe(&poolmgrv1alpha1.SubscribeRequest{
			Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"},
		}, stream)
	}()

	e1 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	if err := st.AppendEvent(ctx, e1); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	got := recvEvent(t, stream)
	if got.GetId() != e1.GetId() {
		t.Fatalf("live event id = %d, want %d", got.GetId(), e1.GetId())
	}

	cancel()
	<-done
}

func TestSubscribe_PoolFilterExcludesOtherPools(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	ea := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	if err := st.AppendEvent(ctx, ea); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	eb := sampleEvent("pool-b", "default", "vm-2", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	if err := st.AppendEvent(ctx, eb); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := newFakeEventStream(streamCtx)

	s := api.NewEventsServer(st, 10*time.Millisecond, 0)
	done := make(chan error, 1)
	go func() {
		done <- s.Subscribe(&poolmgrv1alpha1.SubscribeRequest{
			Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"},
		}, stream)
	}()

	got := recvEvent(t, stream)
	if got.GetId() != ea.GetId() {
		t.Fatalf("event id = %d, want %d", got.GetId(), ea.GetId())
	}
	assertNoEvent(t, stream)

	cancel()
	<-done
}

func TestSubscribe_NoFilterReceivesAllPools(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	ea := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	if err := st.AppendEvent(ctx, ea); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	eb := sampleEvent("pool-b", "default", "vm-2", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	if err := st.AppendEvent(ctx, eb); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := newFakeEventStream(streamCtx)

	s := api.NewEventsServer(st, 10*time.Millisecond, 0)
	done := make(chan error, 1)
	go func() {
		done <- s.Subscribe(&poolmgrv1alpha1.SubscribeRequest{}, stream)
	}()

	got1 := recvEvent(t, stream)
	got2 := recvEvent(t, stream)
	if got1.GetId() != ea.GetId() || got2.GetId() != eb.GetId() {
		t.Fatalf("got events %d,%d, want %d,%d", got1.GetId(), got2.GetId(), ea.GetId(), eb.GetId())
	}

	cancel()
	<-done
}

func TestSubscribe_DrainsMultipleBatchesBeforePolling(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	const numEvents = 5
	want := make([]int64, numEvents)
	for i := 0; i < numEvents; i++ {
		e := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
		if err := st.AppendEvent(ctx, e); err != nil {
			t.Fatalf("AppendEvent: %v", err)
		}
		want[i] = e.GetId()
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := newFakeEventStream(streamCtx)

	// A batch size smaller than numEvents, paired with a poll interval far
	// longer than the test timeout, means the only way every event can
	// arrive before recvEvent's 2s deadline is if Subscribe keeps draining
	// full batches instead of waiting for a tick between them.
	s := api.NewEventsServer(st, time.Hour, 2)
	done := make(chan error, 1)
	go func() {
		done <- s.Subscribe(&poolmgrv1alpha1.SubscribeRequest{
			Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"},
		}, stream)
	}()

	for i, wantID := range want {
		got := recvEvent(t, stream)
		if got.GetId() != wantID {
			t.Fatalf("event[%d] id = %d, want %d", i, got.GetId(), wantID)
		}
	}

	cancel()
	<-done
}

func TestSubscribe_ContextCancelReturnsPromptly(t *testing.T) {
	st := openTestStore(t)
	streamCtx, cancel := context.WithCancel(context.Background())

	stream := newFakeEventStream(streamCtx)
	s := api.NewEventsServer(st, 10*time.Millisecond, 0)
	done := make(chan error, 1)
	go func() {
		done <- s.Subscribe(&poolmgrv1alpha1.SubscribeRequest{}, stream)
	}()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil after cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe() did not return promptly after context cancellation")
	}
}
