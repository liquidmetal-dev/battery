# DeletePool drains the pool

Related: [issue #112](https://github.com/liquidmetal-dev/battery/issues/112),
[issue #103](https://github.com/liquidmetal-dev/battery/issues/103).

## Context

`PoolAdmin.DeletePool` returns `FAILED_PRECONDITION` whenever the pool has a VM
row in any phase (`internal/api/pooladmin.go`), and the API offers no way to
empty a pool:

- No RPC deletes a specific VM (#103).
- `UpdatePool` does not scale a pool down.
- Replenishment refills the pool after every release (`REPLACE_ON_DELETE`) or
  claim (`IMMEDIATE_ON_LEASE`).

A pool with an `AVAILABLE` VM is therefore effectively undeletable, and its
microVMs keep running on their flintlock hosts. Found by the acceptance suite in
`liquidmetal-dev/acceptance-tests`.

This spec makes `DeletePool` drain the pool itself.

Decisions:

- **Leased VMs:** `DeletePool` refuses while any VM is leased, unless the
  request sets `force`. Unleased VMs (available, provisioning, quarantined,
  deleting) are always drained.
- **Atomic tombstone:** one store transaction removes the pool row and marks its
  VMs `DELETING`. Flintlock deletes are attempted inline; the sweeper's existing
  `DELETING` retry finishes any that fail. `OK` means "the pool is gone and its
  VMs are deleted or queued for deletion".

Out of scope:

- A per-VM `DeleteVM` RPC and `UpdatePool` scale-down (#103). Neither fixes
  this on its own, because a client draining a pool VM by VM still races
  replenishment.
- A durable `deleting` pool state with a graceful, asynchronous drain that
  waits for leases to end.

## Design

### API (`api/proto/poolmgr/v1alpha1/`)

- `pooladmin.proto`: `DeletePoolRequest` gains `bool force = 2`. The RPC's
  comment documents the contract: unleased VMs are drained;
  `FAILED_PRECONDITION` if any VM is leased, unless `force`.
- `types.proto`: new `EventType` `VM_DELETED_ON_POOL_DELETE = 11`.
- Generated code is refreshed with `hack/generate-proto.sh`.

### Store (`internal/store`)

A new method and error. No schema migration is needed: no foreign key ties VMs
or leases to a pool row.

```go
// ErrPoolHasLeasedVMs is returned by DeletePoolAndMarkVMs when the pool has a
// leased VM and force was not set.
var ErrPoolHasLeasedVMs = errors.New("store: pool has leased vms")

// DeletePoolAndMarkVMs deletes the pool row, marks every VM of the pool
// DELETING (clearing its lease_id) and deletes the pool's lease rows, all in
// one transaction. It returns the VMs as marked. Returns ErrNotFound if the
// pool doesn't exist. Unless force is set, returns ErrPoolHasLeasedVMs and
// changes nothing if any VM is LEASED or PRE_LEASE_HOOK_RUNNING.
DeletePoolAndMarkVMs(ctx context.Context, name, namespace string, force bool) ([]*poolmgrv1alpha1.VMRecord, error)
```

The leased check looks at VM phase, not lease rows, because `ClaimVM` marks the
VM `LEASED` (via `ClaimAvailableVM`) before it creates the lease row. Because
the method is a single transaction it is atomic against `ClaimAvailableVM`: a
claim either commits first, in which case the delete refuses, or finds no
`AVAILABLE` VM.

The existing `Store.DeletePool` is unchanged.

### Reconciler lifecycle (`internal/poolmanager`, `internal/api`)

`Manager.StopReconciler` cancels the reconciler but does not wait for it. An
in-flight `Provision` could then insert a VM row after the tombstone
transaction, and under `HookFailurePolicy_QUARANTINE` its cancellation would
leave that row `QUARANTINED` in a pool that no longer exists.

- `reconcilerHandle` gains a `done` channel, closed when the reconciler's
  goroutine exits.
- New `Manager.StopReconcilerAndWait(ctx, name, namespace) error` cancels and
  forgets the reconciler, then waits for `done` or for `ctx`, returning
  `ctx.Err()` if the wait is cut short. It also waits for any reconciler
  stopped earlier for the same pool that has not exited yet: `UpdatePool`
  stops one without waiting, and it may still be unwinding a cancelled
  `Provision`. It is a no-op returning nil when there is nothing to stop or
  wait for. A cancelled `Provision` settles within `hookFailureCleanupTimeout`
  (30s).
- `api.PoolLifecycle` gains the same method; `NoopPoolLifecycle` implements it
  as a no-op.
- `UpdatePool` keeps using the non-waiting `StopReconciler`.

### `DeletePool` flow (`internal/api/pooladmin.go`)

`NewPoolAdminServer` gains a `*flintlockclient.Pool` parameter, passed from
`cmd/poolmgrd/main.go`. Under the existing per-pool lock:

1. `GetPool`. `NOT_FOUND` if the pool is missing. The spec is kept for emitting
   events and for restarting the reconciler.
2. Pre-check with `ListVMsByPool`. Without `force`, any VM in `LEASED` or
   `PRE_LEASE_HOOK_RUNNING` fails the RPC with `FAILED_PRECONDITION`
   ("pool ns/name has N leased VMs, release them or delete with force"). The
   reconciler is not touched on this path.
3. `StopReconcilerAndWait`. If the wait fails, the reconciler is started again
   and the context error is returned.
4. `DeletePoolAndMarkVMs`. `ErrPoolHasLeasedVMs` here means a claim landed
   after step 2: the reconciler is started again and the RPC fails with
   `FAILED_PRECONDITION`. Any other error restarts the reconciler and fails
   with `INTERNAL`.
5. On a context detached from the RPC's cancellation: emit
   `VM_DELETED_ON_POOL_DELETE` for every returned VM, then call
   `reconciler.EnsureVMDeleted` for each of them concurrently, each bounded by
   its own timeout. Events go first and deletes run side by side so that one
   unresponsive flintlock host cannot hold up the other VMs or cost them their
   event (the sweeper emits nothing for a VM whose pool is gone). A failed
   delete is logged and the VM is left `DELETING` for
   `Sweeper.retryPendingDeletions`, which already deletes `DELETING` VMs whose
   pool row is gone.
6. Return `OK`.

A consumer holding a lease that was force-deleted gets `NOT_FOUND` from its
next `Heartbeat` or `ReleaseVM`.

### CLI (`internal/poolmgrctl/pool.go`)

`poolmgrctl pool delete` gains `--force`, which sets `DeletePoolRequest.force`.
The command's help text describes the drain. Event output prints the enum name,
so the new event type needs no CLI change.

### Docs

`docs/runbooks/e2e-manual-verification.md` is updated: `DeletePool` works on a
populated pool, with a note on `force` and the new event.

### Accepted limitations

- A microVM on an unreachable flintlock host outlives the `OK` until the
  sweeper reaches it.
- If a pool is recreated under the same name before the sweeper finishes a
  straggler, `FinishVMDeletion` attributes that deletion to the new pool: the
  event type is wrong, and a `REPLACE_ON_DELETE` pool provisions one extra VM.
- With `force`, a `ClaimVM` whose pre-lease hook fails because the pool
  delete took its microVM away reports `INTERNAL` and emits `VM_HOOK_FAILED`,
  although nothing was wrong with the hook. The VM is still deleted.

A `ClaimVM` or `ReleaseVM` racing the delete used to be able to flip a
`DELETING` VM back to a live phase, lease a VM that was already gone, or fail
although cleanup succeeded. #116 closed those: the store no longer moves a VM
out of `DELETING`, `ClaimVM` commits the VM's move to `LEASED` and its lease
in one transaction and returns `ABORTED` if the VM was deleted first, and
`ReleaseVM` returns `OK` when the pool delete finished its cleanup.

### Files touched

- `api/proto/poolmgr/v1alpha1/pooladmin.proto`, `types.proto`, generated code
- `internal/store/store.go`, `sqlite.go`
- `internal/poolmanager/manager.go`
- `internal/api/pooladmin.go`
- `cmd/poolmgrd/main.go`
- `internal/poolmgrctl/pool.go`
- `docs/runbooks/e2e-manual-verification.md`

### Testing

- `internal/store`: the tombstone marks VMs `DELETING` and removes the pool and
  its leases; it refuses, changing nothing, when a VM is `LEASED` or
  `PRE_LEASE_HOOK_RUNNING` unless `force`; `ErrNotFound` for a missing pool.
- `internal/poolmanager`: `StopReconcilerAndWait` returns only after the runner
  exits, and returns the context error on timeout.
- `internal/api`:
  - available, provisioning and quarantined VMs are deleted from flintlock and
    the store, events are emitted, and the reconciler is stopped;
  - a leased VM without `force` gives `FAILED_PRECONDITION`, nothing is
    deleted, and the reconciler is not stopped;
  - a leased VM with `force` removes the VM and its lease;
  - a failing flintlock delete still gives `OK` with the pool gone and the VM
    left `DELETING`, and a later `Sweeper.Tick` removes it;
  - a tombstone refusal after the stop restarts the reconciler.
- `internal/poolmgrctl`: `--force` reaches the request.

## Verification

- `hack/generate-proto.sh` produces no diff beyond the two proto changes.
- `go build ./...`, `go test ./...` and `golangci-lint run` pass.
- End to end, following the issue's reproduction: create a `size: 1`
  `REPLACE_ON_DELETE` pool, wait for `available_count == 1`, and run
  `poolmgrctl pool delete`. It succeeds, `GetPool` returns `NOT_FOUND`, the
  microVM is gone from its flintlock host, and no replacement appears. With an
  active lease the delete is refused without `--force` and succeeds with it.
