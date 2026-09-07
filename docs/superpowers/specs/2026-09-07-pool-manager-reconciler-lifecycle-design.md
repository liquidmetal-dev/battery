# Dynamic per-pool Reconciler lifecycle (PoolManager)

Related: [issue #40](https://github.com/liquidmetal-dev/battery/issues/40) (split out of #36).

## Context

`internal/reconciler.New` builds a `Reconciler` for a single pool, and
`Reconciler.Run(ctx)` drives its ticker/sweeper loop, but nothing in `poolmgrd`
actually starts or stops one per pool today. `cmd/poolmgrd/main.go` wires up the
gRPC/metrics servers but never calls `reconciler.New`/`Run`. `api.LeaseServer`'s
`ReconcilerNotifier` is wired to `NoopNotifier` by default — the seam exists
(`NotifyVMClaimed`/`NotifyVMDeleted` are already called from `ClaimVM`/`ReleaseVM`)
but nothing implements it for real. Net effect: `IMMEDIATE_ON_LEASE`/
`REPLACE_ON_DELETE` replenishment and the reconciler's tick loop never run
against live pools.

This spec adds a small `PoolManager` component that owns the lifecycle of one
`Reconciler` goroutine per pool: starting them at `poolmgrd` startup (from
existing store state) and on `CreatePool`, stopping them on `DeletePool`, and
routing lease events to the right pool's reconciler as the real
`ReconcilerNotifier` implementation.

Out of scope:
- Multi-replica/HA coordination (#17) — single-instance ownership of all pool
  reconcilers is fine for v1.
- Starting/stopping the `Sweeper` (`internal/reconciler/sweeper.go`) — it is
  also currently dead code (never started in `main.go`), but wiring it up is a
  separate, single-instance (not per-pool) lifecycle concern. Tracked as a
  follow-up issue, filed separately from this change.

## Design

### `internal/poolmanager` (new package)

A `Manager` type owns per-pool `Reconciler` lifecycle:

- Internal state: `sync.Mutex`-protected `map[poolKey]*reconcilerHandle`,
  keyed by `(name, namespace)`. Each handle holds a per-pool
  `context.CancelFunc` and the running `*reconciler.Reconciler`.
- Implements `api.ReconcilerNotifier`:
  - `NotifyVMClaimed(poolName, poolNamespace string)`
  - `NotifyVMDeleted(poolName, poolNamespace string)`

  Both look up the handle and forward to the pool's
  `*reconciler.Reconciler.NotifyVMClaimed`/`NotifyVMDeleted`. No-op if the pool
  isn't found (e.g. a race with a concurrent delete).
- `StartReconciler(spec *poolmgrv1alpha1.PoolSpec) error`: derives a child
  context from the Manager's root context, constructs a
  `*reconciler.Reconciler` via `reconciler.New(...)`, launches
  `go r.Run(childCtx)` tracked by a `sync.WaitGroup`, and stores the handle.
- `StopReconciler(name, namespace string)`: looks up the handle, cancels its
  context, and removes it from the map. Fire-and-forget — does not block
  waiting for the goroutine to exit (matches `Run`'s ctx.Err()-on-cancel
  contract).
- `Seed(ctx context.Context) error`: calls `store.ListPools` and
  `StartReconciler` for each — invoked once at `poolmgrd` startup.
- `Run(ctx context.Context) error`: blocks until `ctx.Done()`, then cancels
  all remaining per-pool contexts and waits on the `WaitGroup` before
  returning, so shutdown is clean. This is the method launched as a goroutine
  in `main()`.

### Per-pool failure handling

When a pool's `Run` returns, `errors.Is(err, context.Canceled)` is the
expected path (triggered by `StopReconciler` or process shutdown) — no
error-level log needed. Any other error is currently unreachable given
`Reconciler.Run`'s contract (it only ever returns `ctx.Err()`; internal errors
are already logged inside the reconciler, not propagated), but defensively:
log at error level with pool name/namespace, increment a new
`poolmanager_reconciler_unexpected_exit_total` counter (labeled
`pool_name`/`pool_namespace`, following the existing `internal/metrics`
convention), and leave that pool stopped. **No automatic restart** — this
keeps the component simple for a path that shouldn't occur, and surfaces it
as an operational signal rather than silently self-healing.

### Wiring changes

- `internal/api/pooladmin.go`: `PoolAdminServer` gets a new field of a small
  interface (keeps `internal/api` decoupled from `internal/poolmanager`
  concretely):
  ```go
  type PoolLifecycle interface {
      StartReconciler(spec *poolmgrv1alpha1.PoolSpec) error
      StopReconciler(name, namespace string)
  }
  ```
  Threaded through `NewPoolAdminServer(st store.Store, poolMgr PoolLifecycle)`.
  `CreatePool` calls `poolMgr.StartReconciler(spec)` immediately after
  `store.CreatePool` succeeds (before returning the response). `DeletePool`
  calls `poolMgr.StopReconciler(name, ns)` immediately after
  `store.DeletePool` succeeds.
- `internal/api/lease.go`: no code changes needed — `NewLeaseServer` already
  accepts a `ReconcilerNotifier`; `main.go` just needs to stop passing `nil`.
- `cmd/poolmgrd/main.go`: construct `poolmanager.New(st, flint, reg, ...)`
  before `buildGRPCServer`; call `poolMgr.Seed(runCtx)` at startup; pass
  `poolMgr` into `NewPoolAdminServer` and as the real `notifier` into
  `NewLeaseServer`. Launch `go func(){ errCh <- poolMgr.Run(runCtx) }()` as a
  third `pending++` participant alongside the existing gRPC/metrics
  goroutines, using the same `errCh`-drain + `stop()`-on-first-real-error
  pattern already in place for graceful shutdown.

### Files touched

- `internal/poolmanager/manager.go` (new) — `Manager` type and methods above.
- `internal/poolmanager/manager_test.go` (new).
- `internal/api/pooladmin.go` — add `PoolLifecycle` field/interface, call
  sites in `CreatePool`/`DeletePool`.
- `internal/api/pooladmin_test.go` — add cases asserting `StartReconciler`/
  `StopReconciler` are invoked via a mock `PoolLifecycle`.
- `cmd/poolmgrd/main.go` — construct/seed/wire/launch the `Manager`.
- `internal/metrics/metrics.go` — add
  `RecordReconcilerUnexpectedExit(poolName, poolNamespace string)` (or
  similar, matching existing `Record*` naming).

### Testing

- `internal/poolmanager/manager_test.go`, mirroring
  `internal/reconciler/reconciler_test.go`'s patterns (in-memory/fake
  `store.Store`, cancellable contexts, `errors.Is(err, context.Canceled)`
  assertions):
  - `StartReconciler` then `StopReconciler` — handle added then removed from
    the map, goroutine observably stops (e.g. via a fake reconciler run func
    injected for the test, or by driving a real `Reconciler` against a fast
    tick interval and asserting it stops ticking after cancel).
  - `Seed` starts one reconciler per pool returned by `ListPools`.
  - `NotifyVMClaimed`/`NotifyVMDeleted` forward to the right pool's
    reconciler; no-op (no panic) for an unknown pool.
  - `Run(ctx)` cancels all children and returns after `ctx` is cancelled.
- `internal/api/pooladmin_test.go`: table test asserting `CreatePool` calls
  `StartReconciler` with the created spec only on success (not on validation
  failure), and `DeletePool` calls `StopReconciler` only after the store
  delete succeeds.
- Manual/integration verification: run `poolmgrd` locally, create a pool via
  the admin API, confirm (via logs/metrics) a reconciler tick loop is running
  for it; delete the pool and confirm the loop stops; restart `poolmgrd` with
  an existing pool in the store and confirm `Seed` picks it up.

## Verification

- `go test ./internal/poolmanager/... ./internal/api/... ./cmd/poolmgrd/...`
- `go build ./...`
- Manual run of `poolmgrd` as described above (create/delete/restart pool,
  observe reconciler start/stop via logs and the new metric).

## Follow-up (not part of this change)

File a GitHub issue for wiring up `Sweeper` startup/shutdown in `poolmgrd`,
analogous to this PoolManager but for the single all-pools lease-sweep loop.
