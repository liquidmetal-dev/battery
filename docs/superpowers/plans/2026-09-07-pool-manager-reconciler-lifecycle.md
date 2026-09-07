# Dynamic per-pool Reconciler lifecycle (PoolManager) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every pool a running `reconciler.Reconciler` goroutine for its whole lifetime, so `IMMEDIATE_ON_LEASE`/`REPLACE_ON_DELETE` replenishment and the tick loop actually run against live pools.

**Architecture:** A new `internal/poolmanager` package's `Manager` type owns a map of per-pool reconciler goroutines, keyed by (name, namespace). `PoolAdminServer.CreatePool`/`DeletePool` call `Manager.StartReconciler`/`StopReconciler`; `LeaseServer` gets `Manager` as its `ReconcilerNotifier`; `poolmgrd`'s `main()` seeds `Manager` from the store at startup and runs it alongside the existing gRPC/metrics goroutines.

**Tech Stack:** Go, `log/slog`, `sync`, Prometheus client_golang, SQLite-backed `store.Store` (existing).

**Spec:** `docs/superpowers/specs/2026-09-07-pool-manager-reconciler-lifecycle-design.md`

## Global Constraints

- Commit messages follow Conventional Commits (`feat:`, `fix:`, `test:`, etc.) per this repo's `AGENTS.md`. No agent footers in commit messages or PR descriptions per that same file (note: this session has separate, overriding attribution instructions for commit trailers — apply those instead where they conflict).
- No automatic restart of a reconciler whose `Run` returns unexpectedly — log at error level and increment a metric instead (spec's Error Handling section).
- `Sweeper` startup/shutdown is explicitly out of scope for this plan.

---

### Task 1: Add `RecordReconcilerUnexpectedExit` metric

**Files:**
- Modify: `internal/metrics/metrics.go`
- Test: `internal/metrics/metrics_test.go`

**Interfaces:**
- Produces: `func (r *Registry) RecordReconcilerUnexpectedExit(poolName, poolNamespace string)` — used by `internal/poolmanager.Manager` (Task 2).

- [ ] **Step 1: Write the failing test**

Append to `internal/metrics/metrics_test.go`:

```go
func TestRecordReconcilerUnexpectedExit(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.RecordReconcilerUnexpectedExit("pool-a", "default")
	reg.RecordReconcilerUnexpectedExit("pool-a", "default")

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_reconciler_unexpected_exit_total{pool_name="pool-a",pool_namespace="default"} 2`)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/metrics/... -run TestRecordReconcilerUnexpectedExit -v`
Expected: FAIL — `reg.RecordReconcilerUnexpectedExit undefined`

- [ ] **Step 3: Implement**

In `internal/metrics/metrics.go`, add a field to `metricSet`:

```go
type metricSet struct {
	vmClaimsTotal                 *prometheus.CounterVec
	vmReleasesTotal                *prometheus.CounterVec
	provisionDuration               *prometheus.HistogramVec
	hookDuration                    *prometheus.HistogramVec
	hookFailuresTotal                *prometheus.CounterVec
	leaseDuration                    *prometheus.HistogramVec
	reconcilerUnexpectedExitsTotal   *prometheus.CounterVec
}
```

In `newMetricSet`, add the constructor and register it:

```go
		reconcilerUnexpectedExitsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "poolmgr_reconciler_unexpected_exit_total",
			Help: "Total number of times a pool's Reconciler.Run returned with an error other than context.Canceled, labeled by pool_name/pool_namespace. Expected to stay at 0 - see internal/poolmanager.",
		}, []string{"pool_name", "pool_namespace"}),
```

(add `m.reconcilerUnexpectedExitsTotal` to the `reg.MustRegister(...)` call)

Add the method, near `RecordHookFailure`:

```go
// RecordReconcilerUnexpectedExit increments poolmgr_reconciler_unexpected_exit_total
// for poolName/poolNamespace. A reconciler's Run loop is only ever expected to
// return via context cancellation; any other return is unexpected and is not
// retried (see internal/poolmanager.Manager.StartReconciler).
func (r *Registry) RecordReconcilerUnexpectedExit(poolName, poolNamespace string) {
	r.metrics.reconcilerUnexpectedExitsTotal.WithLabelValues(poolName, poolNamespace).Inc()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/metrics/... -run TestRecordReconcilerUnexpectedExit -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/metrics/metrics.go internal/metrics/metrics_test.go
git commit -m "feat: add poolmgr_reconciler_unexpected_exit_total metric"
```

---

### Task 2: `internal/poolmanager.Manager` — core lifecycle

**Files:**
- Create: `internal/poolmanager/manager.go`
- Test: `internal/poolmanager/manager_test.go` (package `poolmanager`, not `poolmanager_test` — needs to override the unexported `newReconciler` package var to inject a fake, the same reason `internal/reconciler`'s own external test package can't reuse `store`'s unexported test helpers)

**Interfaces:**
- Consumes: `reconciler.New(pool *poolmgrv1alpha1.PoolSpec, st store.Store, flint *flintlockclient.Pool, tickInterval time.Duration, pcfg reconciler.ProvisionConfig, m *metrics.Registry) (*reconciler.Reconciler, error)`, `(*reconciler.Reconciler).Run(ctx) error`, `.NotifyVMClaimed()`, `.NotifyVMDeleted()`; `store.Store.ListPools(ctx) ([]*poolmgrv1alpha1.PoolSpec, error)`; `metrics.Registry.RecordReconcilerUnexpectedExit(poolName, poolNamespace string)` (Task 1).
- Produces (used by Tasks 3 and 4):
  - `func New(ctx context.Context, st store.Store, flint *flintlockclient.Pool, m *metrics.Registry) *Manager`
  - `func (m *Manager) Seed(ctx context.Context) error`
  - `func (m *Manager) StartReconciler(spec *poolmgrv1alpha1.PoolSpec) error`
  - `func (m *Manager) StopReconciler(name, namespace string)`
  - `func (m *Manager) Running(name, namespace string) bool`
  - `func (m *Manager) NotifyVMClaimed(poolName, poolNamespace string)`
  - `func (m *Manager) NotifyVMDeleted(poolName, poolNamespace string)`
  - `func (m *Manager) Run() error`

- [ ] **Step 1: Write the failing tests**

Create `internal/poolmanager/manager_test.go`:

```go
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
	return &poolmgrv1alpha1.PoolSpec{Name: name, Namespace: "default"}
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

func TestManager_StopReconciler_UnknownPoolIsNoop(t *testing.T) {
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

func TestManager_Notify_UnknownPoolIsNoop(t *testing.T) {
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/poolmanager/... -v`
Expected: FAIL to compile — `undefined: New`, `undefined: newReconciler`, `undefined: reconcilerRunner`, etc. (package doesn't exist yet)

- [ ] **Step 3: Implement**

Create `internal/poolmanager/manager.go`:

```go
// Package poolmanager owns the lifecycle of per-pool reconciler.Reconciler
// goroutines: starting one for every pool at poolmgrd startup and on
// PoolAdminServer.CreatePool, stopping it on DeletePool, and routing lease
// events to the right pool's reconciler as api.ReconcilerNotifier.
package poolmanager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// reconcilerRunner is the subset of *reconciler.Reconciler that Manager
// depends on. newReconciler (below) is a package var so tests can substitute
// a fake that doesn't need a real store/flintlock connection.
type reconcilerRunner interface {
	Run(ctx context.Context) error
	NotifyVMClaimed()
	NotifyVMDeleted()
}

// newReconciler builds the reconcilerRunner for a pool. Overridden in tests.
var newReconciler = func(spec *poolmgrv1alpha1.PoolSpec, st store.Store, flint *flintlockclient.Pool, m *metrics.Registry) (reconcilerRunner, error) {
	return reconciler.New(spec, st, flint, 0, reconciler.ProvisionConfig{}, m)
}

type poolKey struct {
	name      string
	namespace string
}

type reconcilerHandle struct {
	runner reconcilerRunner
	cancel context.CancelFunc
}

// Manager owns one reconcilerRunner goroutine per pool: startReconciler
// launches it, stopReconciler cancels and forgets it, and Run tears down
// whatever's left when its root context (given to New) is done.
type Manager struct {
	store   store.Store
	flint   *flintlockclient.Pool
	metrics *metrics.Registry
	rootCtx context.Context

	mu      sync.Mutex
	handles map[poolKey]*reconcilerHandle
	wg      sync.WaitGroup
}

// New returns a Manager whose per-pool reconciler goroutines are children of
// ctx: once ctx is done, Run cancels and waits for all of them. If m is nil,
// a fresh unshared metrics.Registry is used (see reconciler.New).
func New(ctx context.Context, st store.Store, flint *flintlockclient.Pool, m *metrics.Registry) *Manager {
	if m == nil {
		m = metrics.NewRegistry()
	}
	return &Manager{
		store:   st,
		flint:   flint,
		metrics: m,
		rootCtx: ctx,
		handles: make(map[poolKey]*reconcilerHandle),
	}
}

// Seed starts one reconciler for every pool currently in the store. Intended
// to be called once at startup, before Run.
func (m *Manager) Seed(ctx context.Context) error {
	pools, err := m.store.ListPools(ctx)
	if err != nil {
		return fmt.Errorf("poolmanager: list pools: %w", err)
	}
	for _, spec := range pools {
		if err := m.StartReconciler(spec); err != nil {
			return fmt.Errorf("poolmanager: seed %s/%s: %w", spec.GetNamespace(), spec.GetName(), err)
		}
	}
	return nil
}

// StartReconciler starts a reconciler goroutine for spec. Returns an error
// if a reconciler for spec's (name, namespace) is already running.
func (m *Manager) StartReconciler(spec *poolmgrv1alpha1.PoolSpec) error {
	key := poolKey{name: spec.GetName(), namespace: spec.GetNamespace()}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.handles[key]; exists {
		return fmt.Errorf("poolmanager: reconciler for %s/%s already running", key.namespace, key.name)
	}

	runner, err := newReconciler(spec, m.store, m.flint, m.metrics)
	if err != nil {
		return fmt.Errorf("poolmanager: new reconciler for %s/%s: %w", key.namespace, key.name, err)
	}

	childCtx, cancel := context.WithCancel(m.rootCtx)
	m.handles[key] = &reconcilerHandle{runner: runner, cancel: cancel}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := runner.Run(childCtx); err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(m.rootCtx, "poolmanager: reconciler exited unexpectedly", "pool", key.name, "namespace", key.namespace, "error", err)
			m.metrics.RecordReconcilerUnexpectedExit(key.name, key.namespace)
		}
	}()
	return nil
}

// StopReconciler cancels and forgets the reconciler for (name, namespace),
// if one is running. It does not wait for the reconciler's goroutine to
// exit - see Run for the shutdown path that does.
func (m *Manager) StopReconciler(name, namespace string) {
	key := poolKey{name: name, namespace: namespace}

	m.mu.Lock()
	h, ok := m.handles[key]
	if ok {
		delete(m.handles, key)
	}
	m.mu.Unlock()

	if ok {
		h.cancel()
	}
}

// Running reports whether a reconciler for (name, namespace) is currently
// tracked by Manager.
func (m *Manager) Running(name, namespace string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.handles[poolKey{name: name, namespace: namespace}]
	return ok
}

// NotifyVMClaimed implements api.ReconcilerNotifier: it forwards to the
// named pool's reconciler, if one is running. No-op otherwise (e.g. a race
// with StopReconciler).
func (m *Manager) NotifyVMClaimed(poolName, poolNamespace string) {
	if h, ok := m.handle(poolName, poolNamespace); ok {
		h.runner.NotifyVMClaimed()
	}
}

// NotifyVMDeleted implements api.ReconcilerNotifier: see NotifyVMClaimed.
func (m *Manager) NotifyVMDeleted(poolName, poolNamespace string) {
	if h, ok := m.handle(poolName, poolNamespace); ok {
		h.runner.NotifyVMDeleted()
	}
}

func (m *Manager) handle(name, namespace string) (*reconcilerHandle, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.handles[poolKey{name: name, namespace: namespace}]
	return h, ok
}

// Run blocks until Manager's root context (passed to New) is done, then
// cancels every remaining reconciler and waits for their goroutines to exit
// before returning the root context's error. Intended to be launched as its
// own goroutine alongside poolmgrd's other servers.
func (m *Manager) Run() error {
	<-m.rootCtx.Done()

	m.mu.Lock()
	handles := make([]*reconcilerHandle, 0, len(m.handles))
	for k, h := range m.handles {
		handles = append(handles, h)
		delete(m.handles, k)
	}
	m.mu.Unlock()

	for _, h := range handles {
		h.cancel()
	}
	m.wg.Wait()

	return m.rootCtx.Err()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/poolmanager/... -v`
Expected: PASS (all `TestManager_*` tests)

- [ ] **Step 5: Commit**

```bash
git add internal/poolmanager/manager.go internal/poolmanager/manager_test.go
git commit -m "feat: add poolmanager.Manager for per-pool reconciler lifecycle"
```

---

### Task 3: Wire `PoolLifecycle` into `PoolAdminServer`

**Files:**
- Modify: `internal/api/pooladmin.go`
- Modify: `internal/api/pooladmin_test.go` (update all `NewPoolAdminServer` call sites)
- Modify: `internal/api/testutil_test.go` (add `fakePoolLifecycle`)

**Interfaces:**
- Consumes: `internal/poolmanager.Manager` satisfies this task's new `PoolLifecycle` interface structurally (`StartReconciler(spec) error`, `StopReconciler(name, namespace string)`) — no import of `internal/poolmanager` needed in `internal/api`.
- Produces: `func NewPoolAdminServer(st store.Store, poolMgr PoolLifecycle) *PoolAdminServer` (signature change), `type PoolLifecycle interface{...}`, `var NoopPoolLifecycle PoolLifecycle`.

- [ ] **Step 1: Write the failing tests**

Add to `internal/api/testutil_test.go` (near the top, after imports — add `"sync"` to the import block if not already present, it already is):

```go
// fakePoolLifecycle records StartReconciler/StopReconciler calls for tests
// that assert PoolAdminServer's wiring without a real poolmanager.Manager.
type fakePoolLifecycle struct {
	mu       sync.Mutex
	started  []string
	stopped  []string
	startErr error
}

func (f *fakePoolLifecycle) StartReconciler(spec *poolmgrv1alpha1.PoolSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, spec.GetName())
	return f.startErr
}

func (f *fakePoolLifecycle) StopReconciler(name, _ string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, name)
}
```

Append to `internal/api/pooladmin_test.go`:

```go
func TestCreatePool_StartsReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.started) != 1 || lifecycle.started[0] != "pool-a" {
		t.Fatalf("started = %v, want [pool-a]", lifecycle.started)
	}
}

func TestCreatePool_ValidationFailure_DoesNotStartReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, lifecycle)

	spec := samplePool("", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil) // empty name is invalid
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err == nil {
		t.Fatal("expected CreatePool to fail validation")
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.started) != 0 {
		t.Fatalf("started = %v, want none", lifecycle.started)
	}
}

func TestDeletePool_StopsReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}}); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.stopped) != 1 || lifecycle.stopped[0] != "pool-a" {
		t.Fatalf("stopped = %v, want [pool-a]", lifecycle.stopped)
	}
}

func TestDeletePool_VMsStillPresent_DoesNotStopReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}}); err == nil {
		t.Fatal("expected DeletePool to fail with VMs still present")
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.stopped) != 0 {
		t.Fatalf("stopped = %v, want none", lifecycle.stopped)
	}
}
```

Update every existing `api.NewPoolAdminServer(st)` call in `internal/api/pooladmin_test.go` to `api.NewPoolAdminServer(st, nil)` (nil defaults to a no-op lifecycle, added in Step 3 below — mirrors `NewLeaseServer`'s existing `notifier == nil` → `NoopNotifier{}` pattern):

```bash
sed -i 's/api\.NewPoolAdminServer(st)/api.NewPoolAdminServer(st, nil)/g' internal/api/pooladmin_test.go
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/api/... -v`
Expected: FAIL to compile — `too many arguments in call to api.NewPoolAdminServer`, `undefined: fakePoolLifecycle`, etc.

- [ ] **Step 3: Implement**

In `internal/api/pooladmin.go`, add the interface, the no-op default, the new field, and update `NewPoolAdminServer`:

```go
// PoolLifecycle starts and stops a pool's Reconciler in response to
// CreatePool/DeletePool succeeding. Satisfied structurally by
// *poolmanager.Manager; kept narrow here so internal/api doesn't need to
// import internal/poolmanager.
type PoolLifecycle interface {
	StartReconciler(spec *poolmgrv1alpha1.PoolSpec) error
	StopReconciler(name, namespace string)
}

// NoopPoolLifecycle implements PoolLifecycle by doing nothing. Used when
// poolMgr is nil, e.g. in most unit tests.
type NoopPoolLifecycle struct{}

// StartReconciler does nothing and always succeeds.
func (NoopPoolLifecycle) StartReconciler(*poolmgrv1alpha1.PoolSpec) error { return nil }

// StopReconciler does nothing.
func (NoopPoolLifecycle) StopReconciler(string, string) {}
```

Update the struct and constructor:

```go
type PoolAdminServer struct {
	poolmgrv1alpha1.UnimplementedPoolAdminServer

	store   store.Store
	poolMgr PoolLifecycle
}

// NewPoolAdminServer returns a PoolAdminServer backed by st. If poolMgr is
// nil, NoopPoolLifecycle{} is used.
func NewPoolAdminServer(st store.Store, poolMgr PoolLifecycle) *PoolAdminServer {
	if poolMgr == nil {
		poolMgr = NoopPoolLifecycle{}
	}
	return &PoolAdminServer{store: st, poolMgr: poolMgr}
}
```

Add `"log/slog"` to the import block, then update `CreatePool` (insert after the existing `store.CreatePool` call, before the final `return`):

```go
	if err := s.store.CreatePool(ctx, spec); err != nil {
		return nil, status.Errorf(codes.Internal, "create pool: %v", err)
	}

	// A start failure here is unreachable in practice (spec's replenishment
	// strategy was already validated above), but if it ever happens, the
	// pool itself was created successfully - failing the RPC would misreport
	// that to the caller. Log it as an operational signal instead; the pool
	// will simply have no reconciler running until poolmgrd restarts (which
	// re-seeds every pool) or the pool is deleted and recreated.
	if err := s.poolMgr.StartReconciler(spec); err != nil {
		slog.ErrorContext(ctx, "pooladmin: start reconciler failed", "pool", spec.GetName(), "namespace", spec.GetNamespace(), "error", err)
	}

	return &poolmgrv1alpha1.Pool{Spec: spec, Status: &poolmgrv1alpha1.PoolStatus{}}, nil
```

And `DeletePool` (insert after the existing `store.DeletePool` call, before the final `return`):

```go
	if err := s.store.DeletePool(ctx, name, ns); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", ns, name)
		}
		return nil, status.Errorf(codes.Internal, "delete pool: %v", err)
	}

	s.poolMgr.StopReconciler(name, ns)
	return &emptypb.Empty{}, nil
```

Update the one production call site, `cmd/poolmgrd/main.go` line ~119, from `api.NewPoolAdminServer(st)` to `api.NewPoolAdminServer(st, poolMgr)` — this is completed in Task 4, not here; for this task, `go vet`/`go build ./internal/...` is enough (the `cmd/poolmgrd` package will fail to build until Task 4, which is expected and fixed there).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/api/... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/api/pooladmin.go internal/api/pooladmin_test.go internal/api/testutil_test.go
git commit -m "feat: wire PoolLifecycle into PoolAdminServer CreatePool/DeletePool"
```

---

### Task 4: Wire `Manager` into `poolmgrd`

**Files:**
- Modify: `cmd/poolmgrd/main.go`

**Interfaces:**
- Consumes: `poolmanager.New(ctx, st, flint, reg) *poolmanager.Manager`, `.Seed(ctx) error`, `.Run() error` (Task 2); `api.NewPoolAdminServer(st, poolMgr) *api.PoolAdminServer` (Task 3, `*poolmanager.Manager` satisfies `api.PoolLifecycle` structurally); `api.NewLeaseServer(st, flint, cfg, notifier, m)` where `notifier` accepts anything satisfying `api.ReconcilerNotifier` (`*poolmanager.Manager` satisfies it via `NotifyVMClaimed`/`NotifyVMDeleted`, added in Task 2).

This task has no new unit tests of its own (it's wiring inside `main()`, which the codebase doesn't unit test); its correctness is verified by `go build` plus the manual verification in Task 5.

- [ ] **Step 1: Modify `buildGRPCServer` to accept and use a `*poolmanager.Manager`**

In `cmd/poolmgrd/main.go`, add the import:

```go
	"github.com/liquidmetal-dev/battery/internal/poolmanager"
```

Change `buildGRPCServer`'s signature and body:

```go
func buildGRPCServer(cfg config.APIServerConfig, st store.Store, flint *flintlockclient.Pool, reg *metrics.Registry, poolMgr *poolmanager.Manager) (*grpc.Server, error) {
	srv, err := server.New(cfg, reg.ServerOptions()...)
	if err != nil {
		return nil, fmt.Errorf("build grpc server: %w", err)
	}

	poolmgrv1alpha1.RegisterPoolAdminServer(srv, api.NewPoolAdminServer(st, poolMgr))
	poolmgrv1alpha1.RegisterLeaseServer(srv, api.NewLeaseServer(st, flint, api.HookExecConfig{}, poolMgr, reg))
	poolmgrv1alpha1.RegisterEventsServer(srv, api.NewEventsServer(st, 0, 0))

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(srv, healthSrv)

	reflection.Register(srv)

	reg.RegisterGRPCServer(srv)

	return srv, nil
}
```

- [ ] **Step 2: Construct, seed, and run the Manager in `main()`**

Replace this block in `main()`:

```go
	reg := metrics.NewRegistry()
	reg.RegisterPoolCollector(st)

	// runCtx is cancelled either by the outer signal-driven ctx, or by us
	// below if one server fails - either way, both listeners shut down
	// together and we drain both results before exiting.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	errCh := make(chan error, 2)
	pending := 1

	go func() {
		errCh <- serveMetrics(runCtx, cfg.MetricsAddr, reg)
	}()

	if cfg.APIServer != nil {
		grpcSrv, err := buildGRPCServer(*cfg.APIServer, st, flint, reg)
		if err != nil {
			log.Fatalf("poolmgrd: %v", err)
		}

		lis, err := net.Listen("tcp", cfg.APIServer.Addr)
		if err != nil {
			log.Fatalf("poolmgrd: listen on %s: %v", cfg.APIServer.Addr, err)
		}

		pending++
		go func() {
			errCh <- serveGRPC(runCtx, grpcSrv, lis)
		}()
	} else {
		log.Println("poolmgrd: no api_server configured, gRPC API is disabled")
	}
```

with:

```go
	reg := metrics.NewRegistry()
	reg.RegisterPoolCollector(st)

	// runCtx is cancelled either by the outer signal-driven ctx, or by us
	// below if one server fails - either way, every goroutine started below
	// shuts down together and we drain all of their results before exiting.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	poolMgr := poolmanager.New(runCtx, st, flint, reg)
	if err := poolMgr.Seed(runCtx); err != nil {
		log.Fatalf("poolmgrd: %v", err)
	}

	errCh := make(chan error, 3)
	pending := 2

	go func() {
		errCh <- serveMetrics(runCtx, cfg.MetricsAddr, reg)
	}()
	go func() {
		errCh <- poolMgr.Run()
	}()

	if cfg.APIServer != nil {
		grpcSrv, err := buildGRPCServer(*cfg.APIServer, st, flint, reg, poolMgr)
		if err != nil {
			log.Fatalf("poolmgrd: %v", err)
		}

		lis, err := net.Listen("tcp", cfg.APIServer.Addr)
		if err != nil {
			log.Fatalf("poolmgrd: listen on %s: %v", cfg.APIServer.Addr, err)
		}

		pending++
		go func() {
			errCh <- serveGRPC(runCtx, grpcSrv, lis)
		}()
	} else {
		log.Println("poolmgrd: no api_server configured, gRPC API is disabled")
	}
```

(`errCh`'s buffer grows from 2 to 3 and `pending` starts at 2 instead of 1, to account for the new `poolMgr.Run()` goroutine; the drain loop below already ranges over `pending` and needs no further change.)

- [ ] **Step 3: Build and vet**

Run: `go build ./... && go vet ./...`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add cmd/poolmgrd/main.go
git commit -m "feat: start poolmanager.Manager in poolmgrd, wire it as the ReconcilerNotifier"
```

---

### Task 5: Manual end-to-end verification

**Files:** none (no code changes — verification only)

- [ ] **Step 1: Run the full test suite**

Run: `go test ./...`
Expected: PASS

- [ ] **Step 2: Build the binary**

Run: `go build -o /tmp/poolmgrd ./cmd/poolmgrd`
Expected: builds cleanly

- [ ] **Step 3: Start poolmgrd against a minimal config**

Create a scratch config (adjust `hosts`/`api_server.addr` to a reachable flintlock instance, or use whatever this repo's existing local dev config/fixture provides — check `README.md`/`docs/` for a documented local-dev flintlock setup before hand-rolling one), then:

```bash
/tmp/poolmgrd -config /path/to/scratch-config.json -db /tmp/poolmgrd-verify.db
```

Expected log line confirming the metrics/gRPC listeners start as before (no change there); no error from `poolMgr.Seed`.

- [ ] **Step 4: Create a pool via the admin API and confirm a reconciler is running**

Use `grpcurl` (or the repo's existing CLI/test client, if any — check `cmd/` for one) to call `PoolAdmin.CreatePool` with a minimal spec. Then:

```bash
curl -s http://localhost:9090/metrics | grep poolmgr_reconciler_unexpected_exit_total
```

Expected: the metric line is absent or at 0 (no unexpected exits) — a running reconciler's tick loop doesn't itself expose a distinct "started" metric, so absence-of-errors plus the pool's `PoolStatus` counts changing over time (via `PoolAdmin.GetPool`) is the signal a reconciler is actively provisioning.

- [ ] **Step 5: Delete the pool and confirm no further activity**

Call `PoolAdmin.DeletePool` (after removing/letting go any VMs it created, per its existing `FailedPrecondition` check), then confirm via logs that no further reconcile activity occurs for that pool.

- [ ] **Step 6: Restart and confirm `Seed` re-attaches to existing pools**

Create a second pool, stop `poolmgrd` (`Ctrl+C`, confirm graceful shutdown logs), restart it against the same `-db`, and confirm (via `PoolAdmin.GetPool`/logs) that the pool's reconciler is running again without needing another `CreatePool` call.
