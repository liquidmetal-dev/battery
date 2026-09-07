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
