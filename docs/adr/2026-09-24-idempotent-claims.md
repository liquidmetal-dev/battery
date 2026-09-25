# ADR: Idempotent claims

Related: [issue #98](https://github.com/liquidmetal-dev/battery/issues/98).

- Status: Accepted
- Date: 2026-09-24

## Context

`Lease.ClaimVM` is the one write in the API that a client cannot safely
retry. A `ClaimVMRequest` carries only a `PoolRef`. Two identical requests
are two claims. Nothing on the server ties a second request to the first.

The call is also slow enough that retries happen in practice. `ClaimVM`
claims a VM, then runs the pool's `pre_lease_commands` through the
guest-agent, then commits the lease, then fetches the VM's network
interfaces from flintlock. A hook that takes longer than the client's
deadline gives the client `DEADLINE_EXCEEDED` while the server goes on to
commit the lease. A dropped connection gives `UNAVAILABLE` with the same
result. In both cases the client does not know whether it holds a lease.

If the client retries, the manager hands out a second VM. The first sits in
`LEASED` with nobody to heartbeat it. It expires after the pool's
`heartbeat_expiry_threshold`, the sweeper deletes it, and replenishment
provisions a replacement. The cost is one warm VM, one provisioning cycle,
and, in a small pool, a `RESOURCE_EXHAUSTED` for the next consumer. The
client cannot recover the lost lease either: `ListLeases` shows every lease,
but no lease records who asked for it.

If the client does not retry, it has no VM, and the lost lease still expires.

The other lease RPCs are already safe to repeat. `Heartbeat` extends the
expiry to now plus the threshold each time. `ReleaseVM` deletes the lease row
if the VM is already gone, and its own comment names idempotency as the
reason. `ClaimVM` is the gap.

## Decision

Add a client-chosen `request_id` to `ClaimVMRequest`. The manager stores it
on the lease. A repeat request with the same `request_id` returns the lease
the first request created, instead of a new one.

### API

```proto
message ClaimVMRequest {
  PoolRef pool = 1;
  // RequestId lets a client retry ClaimVM safely. The manager stores it on
  // the lease it creates. A later ClaimVM with the same request_id returns
  // that lease again, for as long as it exists, rather than a new one.
  //
  // Clients that retry ClaimVM must set it. A UUID is a good choice. Empty
  // means the request is not retriable, which is the behaviour before this
  // field existed. At most 255 bytes.
  string request_id = 2;
}

message LeaseRecord {
  // ... existing fields ...
  // RequestId is the ClaimVMRequest.request_id that created this lease.
  // Empty if the client did not set one.
  string request_id = 8;
}
```

The field name follows Google's AIP-155, which names the same idea
`request_id`. That guide is the closest thing to a convention for gRPC APIs,
and this repository already publishes its protos to the Buf Schema Registry
where AIP naming is common.

### Semantics

1. `ClaimVM` with an empty `request_id` behaves as today.
2. `ClaimVM` with a `request_id` longer than 255 bytes fails with
   `INVALID_ARGUMENT`.
3. Before it claims a VM, the manager looks up a lease with that
   `request_id`.
   - If one exists and its pool matches the request, the manager rebuilds
     the `ClaimVMResponse` from the lease and its VM record, fetches the
     network interfaces from flintlock as it does today, and returns it. It
     does not touch the lease's expiry. A replay is not a heartbeat.
   - If one exists and its pool does not match, the manager fails with
     `INVALID_ARGUMENT`. The same `request_id` with a different request is a
     client bug, and a silent success would hide it.
   - If none exists, the manager claims a VM as it does today and writes
     `request_id` on the new lease row.
4. The lease row is the record of the request. When the lease ends, by
   `ReleaseVM` or by expiry, the `request_id` is free again. There is no
   separate key table and nothing extra to sweep.
5. Two requests with the same `request_id` can pass the lookup at the same
   time. Both claim a VM and run the pre-lease hook. The `UNIQUE` index on
   `request_id` rejects the second `CreateLease`. The loser sets its VM back
   to `AVAILABLE`, and returns the winner's lease as in step 3. The pre-lease
   hook runs again on that VM when it is next claimed, which is what the hook
   is for.
6. A claim that fails after it took a VM but before it wrote the lease (a
   hook failure, a store error) leaves no lease and therefore no
   `request_id`. The retry is a fresh claim. That is correct: the first
   attempt applied `hook_failure_policy` to its VM and handed nothing out.

### Persistence

```sql
ALTER TABLE leases ADD COLUMN request_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_leases_request_id
    ON leases (request_id) WHERE request_id IS NOT NULL;
```

The `Store` interface gains `GetLeaseByRequestID`. `CreateLease` returns a
new `ErrDuplicateRequestID` when the index rejects a row.

This is the first change to an existing table since the store was written.
`schema.sql` is applied with `CREATE TABLE IF NOT EXISTS`, which cannot add
a column to a database that already exists. The change therefore also
introduces `PRAGMA user_version` and a short list of numbered migrations
that `store.Open` applies in order. Later schema changes use the same list.

### poolmgrctl

`poolmgrctl lease claim` gains `--request-id`. When the flag is unset the
CLI sends an empty value, as today. This is the change `AGENTS.md` requires
for every API change.

### Metrics

`poolmgr_vm_claims_total` gains a `replayed="true|false"` label, so an
operator can see how often clients retry.

## Consequences

Positive:

- A client can retry `ClaimVM` on `UNAVAILABLE` and `DEADLINE_EXCEEDED`
  without leaking VMs. gRPC's built-in retry policy becomes safe to enable
  for this method.
- A client that lost the response can get its lease back by resending the
  same request, instead of waiting for the lease to expire and claiming
  again.
- `ListLeases` shows which request created each lease, which helps an
  operator match a consumer's logs to a lease.
- No new table, no new sweeper, no new lifecycle. The lease already has one.

Negative:

- Clients must generate and keep a `request_id` across retries. A client
  that sends a fresh one per attempt gets the old behaviour. The proto
  comment and the `poolmgrctl` flag help are the only guard against this.
- The `request_id` is only as durable as the lease. A retry that arrives
  after the lease has ended creates a new lease. This window is the
  lease's whole lifetime, at least one `heartbeat_expiry_threshold`, which
  is far longer than any sane retry policy. A client that retries a claim
  after it has released the VM from that claim has a bug this design does
  not try to catch.
- The concurrent-duplicate path (step 5) runs the pre-lease hook on a VM
  and then returns it to the pool. This is rare, and it costs one hook
  run, not a VM.
- The store gets its first migration mechanism. That is a small amount of
  new code and a new test for upgrading a pre-existing database.

## Alternatives considered

- **A `claim_requests` table that reserves the `request_id` before the claim
  starts.** This stops two concurrent duplicates from both running a hook,
  and it can hold the key after the lease ends. It also adds a state
  machine: a row that is "in progress" when the process crashes poisons its
  key forever unless something sweeps it, and a retention window for
  finished rows needs its own sweep. The lease row gives the same guarantee
  for the cases that matter, with no new lifecycle.
- **Derive the key from transport identity.** Tie a claim to the caller's
  mTLS certificate or basic-auth token plus the pool. The design doc chose
  an opaque lease token over transport identity on purpose, so that one
  client can hold many leases. A per-identity key would allow only one
  outstanding claim per client per pool.
- **Make `ClaimVM` fast enough that retries stop happening.** Run pre-lease
  hooks asynchronously and let the client poll. This changes the API's
  contract and does nothing for a dropped connection. It may still be worth
  doing for latency, as a separate decision.
- **Name the field `idempotency_key`.** More self-describing, and it is
  what Stripe and others use over HTTP. `request_id` is the gRPC convention
  and the two are the same idea. The proto comment says what it does.
- **Return `ALREADY_EXISTS` on a replay instead of the lease.** Tells the
  client the truth but leaves it with no way to get the `lease_id` it
  lost. Returning the lease is the point.

## Out of scope

- **The reconciler's own `CreateMicroVM` call to flintlock.** If flintlock
  creates the VM but the reply is lost, the manager has no record of the
  VM, and it lives on as an orphan on the host. Flintlock's `CreateMicroVM`
  has no idempotency field of its own, so a fix needs either a change in
  flintlock or a reconcile pass that lists each host's VMs, matches them by
  label to the manager's records, and adopts or deletes the strays. That is
  a separate decision.
- Idempotency for `PoolAdmin` writes. A repeated `CreatePool` fails with
  `ALREADY_EXISTS` and a repeated `DeletePool` with `NOT_FOUND`. Both tell
  the caller the truth and change nothing. `UpdatePool` is safe to repeat.
- Rate limiting and per-client quotas on `ClaimVM` (#72).
