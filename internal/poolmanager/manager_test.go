package poolmanager

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// fakeRunner is a reconcilerRunner double: Run blocks until its ctx is done,
// then returns runErr (or ctx.Err() if runErr is unset). NotifyVMClaimed and
// NotifyVMDeleted just count calls.
type fakeRunner struct {
	mu        sync.Mutex
	cancelled bool
	claimed   int
	deleted   int
	runErr    error
}

func (f *fakeRunner) Run(ctx context.Context) error {
	<-ctx.Done()

	f.mu.Lock()
	f.cancelled = true
	err := f.runErr
	f.mu.Unlock()

	if err != nil {
		return err
	}
	return ctx.Err()
}

func (f *fakeRunner) NotifyVMClaimed() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimed++
}

func (f *fakeRunner) NotifyVMDeleted() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted++
}

func (f *fakeRunner) wasCancelled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancelled
}

// withFakeReconciler overrides the package-level newReconciler var for the
// duration of the test, so StartReconciler builds fakeRunners (recorded into
// fakes, keyed by pool name) instead of real reconciler.Reconcilers.
func withFakeReconciler(t *testing.T, fakes map[string]*fakeRunner) {
	t.Helper()
	orig := newReconciler
	newReconciler = func(spec *poolmgrv1alpha1.PoolSpec, _ store.Store, _ *flintlockclient.Pool, _ *metrics.Registry) (reconcilerRunner, error) {
		f := &fakeRunner{}
		fakes[spec.GetName()] = f
		return f, nil
	}
	t.Cleanup(func() { newReconciler = orig })
}

func testPool(name string) *poolmgrv1alpha1.PoolSpec {
	return &poolmgrv1alpha1.PoolSpec{
		Name:                     name,
		Namespace:                "default",
		HeartbeatInterval:        durationpb.New(30 * time.Second),
		HeartbeatExpiryThreshold: durationpb.New(90 * time.Second),
	}
}

func openTestStore(t *testing.T) store.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "poolmgr.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return s
}

func scrapeBody(t *testing.T, reg *metrics.Registry) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	reg.Handler().ServeHTTP(w, req)
	body, err := io.ReadAll(w.Result().Body)
	if err != nil {
		t.Fatalf("read scrape body: %v", err)
	}
	return string(body)
}

func TestManager_StartReconciler_MarksRunning(t *testing.T) {
	fakes := map[string]*fakeRunner{}
	withFakeReconciler(t, fakes)

	m := New(context.Background(), nil, nil, nil)
	if err := m.StartReconciler(testPool("pool-a")); err != nil {
		t.Fatalf("StartReconciler: %v", err)
	}

	if !m.Running("pool-a", "default") {
		t.Fatal("expected pool-a to be running")
	}
}

func TestManager_StartReconciler_DuplicateReturnsError(t *testing.T) {
	fakes := map[string]*fakeRunner{}
	withFakeReconciler(t, fakes)

	m := New(context.Background(), nil, nil, nil)
	if err := m.StartReconciler(testPool("pool-a")); err != nil {
		t.Fatalf("StartReconciler: %v", err)
	}

	if err := m.StartReconciler(testPool("pool-a")); err == nil {
		t.Fatal("expected an error starting an already-running pool")
	}
}

func TestManager_StopReconciler_CancelsAndForgets(t *testing.T) {
	fakes := map[string]*fakeRunner{}
	withFakeReconciler(t, fakes)

	m := New(context.Background(), nil, nil, nil)
	if err := m.StartReconciler(testPool("pool-a")); err != nil {
		t.Fatalf("StartReconciler: %v", err)
	}

	m.StopReconciler("pool-a", "default")

	if m.Running("pool-a", "default") {
		t.Fatal("expected pool-a to no longer be running")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fakes["pool-a"].wasCancelled() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for fake reconciler to observe cancellation")
}

func TestManager_StopReconciler_UnknownPoolIsNoop(_ *testing.T) {
	m := New(context.Background(), nil, nil, nil)
	m.StopReconciler("does-not-exist", "default") // must not panic
}

func TestManager_Seed_StartsOneReconcilerPerStoredPool(t *testing.T) {
	fakes := map[string]*fakeRunner{}
	withFakeReconciler(t, fakes)

	st := openTestStore(t)
	ctx := context.Background()
	for _, name := range []string{"pool-a", "pool-b"} {
		if err := st.CreatePool(ctx, testPool(name)); err != nil {
			t.Fatalf("CreatePool(%s): %v", name, err)
		}
	}

	m := New(ctx, st, nil, nil)
	if err := m.Seed(ctx); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	for _, name := range []string{"pool-a", "pool-b"} {
		if !m.Running(name, "default") {
			t.Fatalf("expected %s to be running after Seed", name)
		}
	}
}

func TestManager_Notify_ForwardsToRunningPool(t *testing.T) {
	fakes := map[string]*fakeRunner{}
	withFakeReconciler(t, fakes)

	m := New(context.Background(), nil, nil, nil)
	if err := m.StartReconciler(testPool("pool-a")); err != nil {
		t.Fatalf("StartReconciler: %v", err)
	}

	m.NotifyVMClaimed("pool-a", "default")
	m.NotifyVMDeleted("pool-a", "default")

	fakes["pool-a"].mu.Lock()
	defer fakes["pool-a"].mu.Unlock()
	if fakes["pool-a"].claimed != 1 || fakes["pool-a"].deleted != 1 {
		t.Fatalf("claimed=%d deleted=%d, want 1 and 1", fakes["pool-a"].claimed, fakes["pool-a"].deleted)
	}
}

func TestManager_Notify_UnknownPoolIsNoop(_ *testing.T) {
	m := New(context.Background(), nil, nil, nil)
	m.NotifyVMClaimed("does-not-exist", "default")
	m.NotifyVMDeleted("does-not-exist", "default") // must not panic
}

func TestManager_Run_CancelsAllChildrenAndReturnsAfterRootDone(t *testing.T) {
	fakes := map[string]*fakeRunner{}
	withFakeReconciler(t, fakes)

	ctx, cancel := context.WithCancel(context.Background())
	m := New(ctx, nil, nil, nil)
	for _, name := range []string{"pool-a", "pool-b"} {
		if err := m.StartReconciler(testPool(name)); err != nil {
			t.Fatalf("StartReconciler(%s): %v", name, err)
		}
	}

	done := make(chan error, 1)
	go func() { done <- m.Run() }()

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Run to return")
	}

	for _, name := range []string{"pool-a", "pool-b"} {
		if m.Running(name, "default") {
			t.Fatalf("expected %s to no longer be running after Run returns", name)
		}
	}
}

func TestManager_UnexpectedExit_RecordsMetric(t *testing.T) {
	fakes := map[string]*fakeRunner{}
	withFakeReconciler(t, fakes)

	reg := metrics.NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := New(ctx, nil, nil, reg)
	if err := m.StartReconciler(testPool("pool-a")); err != nil {
		t.Fatalf("StartReconciler: %v", err)
	}
	fakes["pool-a"].mu.Lock()
	fakes["pool-a"].runErr = errors.New("boom")
	fakes["pool-a"].mu.Unlock()

	m.StopReconciler("pool-a", "default")

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(scrapeBody(t, reg), `poolmgr_reconciler_unexpected_exit_total{pool_name="pool-a",pool_namespace="default"} 1`) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for unexpected-exit metric")
}
