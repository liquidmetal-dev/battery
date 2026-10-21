# ADR: HostAdmin.DrainHost

Related: [issue #141](https://github.com/liquidmetal-dev/battery/issues/141),
[issue #103](https://github.com/liquidmetal-dev/battery/issues/103).
Extends [the host API ADR](2026-09-24-host-api.md) and the cordon from
[PR #83](https://github.com/liquidmetal-dev/battery/pull/83).

- Status: Accepted
- Date: 2026-10-21

## Context

The host API ADR gives an operator one path to take a host out of service:
`CordonHost`, drop the host from each pool spec, wait for `vm_count` to reach
zero, then `RemoveHost`. Cordoning stops new placements, but it does not end
the VMs already on the host. Leased VMs end by release or expiry. Available
VMs stay until a client claims and releases them, and a pool never claims
from one host on purpose. Quarantined and failed VMs stay for good (#103).
So `vm_count` often never reaches zero, and `RemoveHost` refuses.

This blocks scale-down. When the cluster autoscaler removes a Kubernetes
Node that is a flintlock host, the host often holds warm VMs and no leases.
The machine goes away with the VMs on it. The host then stays in the store
for good, because `EnsureVMDeleted` can never reach it.

PR #83 renamed its first `DrainHost` to `CordonHost`, and kept the name
"drain" for cordon plus evict. Since v0.4.0, `DeletePool` deletes a pool's
unleased VMs (`DeletePoolAndMarkVMs`, then `deletePoolVMs`). A host drain
can share that path.

## Decision

Add `HostAdmin.DrainHost`. It cordons the host, marks it draining, and
deletes every VM on it that is not leased. The drain is a state of the host,
not a one-off action: it lasts until `UncordonHost`.

### API

```proto
service HostAdmin {
  rpc DrainHost(DrainHostRequest) returns (Host);
}

message DrainHostRequest {
  string name = 1;
  // When set, replaces the host's cordoned_reason.
  string reason = 2;
}

message Host {
  // Fields 1 to 7 as before.
  bool draining = 8;
  google.protobuf.Timestamp draining_at = 9;
}

enum EventType {
  // Values 0 to 11 as before.
  VM_DELETED_ON_HOST_DRAIN = 12;
}
```

`poolmgrctl host drain <name> [--reason]` wraps the RPC. `host list`, `host
get` and the single-host output show a `DRAINING` column.

### Semantics

- "Not leased" means any phase but `LEASED`, `PRE_LEASE_HOOK_RUNNING` and
  `DELETING`: available, provisioning, create hook running, quarantined and
  failed. This is the complement of `DeletePool`'s leased count.
- `DrainHost` sets `cordoned` and `draining`, and marks the host's unleased
  VMs `DELETING`, in one store transaction (`store.DrainHost`). One `UPDATE`
  checks each VM's phase and writes it. A `ClaimVM` either leases a VM first,
  and the drain leaves it alone, or finds it `DELETING` and skips it.
- It then deletes the marked VMs as `DeletePool` does: an event per VM
  first, then concurrent deletes on a context detached from the caller, each
  bounded by a timeout. A failed delete leaves the VM `DELETING` for the
  Sweeper. The RPC returns once every delete has finished or failed.
- On every tick, the Sweeper marks and deletes the unleased VMs of every
  draining host in the same way (`store.MarkDrainingHostsVMsDeleting`).
- Leased VMs are not touched. They end by release or expiry. Once the last
  one ends, `vm_count` reaches zero and `RemoveHost` succeeds, as before.
- `DrainHost` is idempotent. A repeat call keeps `cordoned_at` and
  `draining_at`, and keeps the reason unless a new one is given. It fails
  with `NOT_FOUND` for an unknown host.
- `UncordonHost` clears both `cordoned` and `draining`. `CordonHost` on a
  draining host keeps the drain. `UpdateHost` and `AddHost` ignore the drain
  fields, as they ignore the cordon fields. `RemoveHost` does not change.
- Each deleted VM gets one `VM_DELETED_ON_HOST_DRAIN` event, written when
  the VM is marked. The Sweeper's retry of a failed delete does not write a
  second event: a `DELETING` VM with no lease on a draining host is known to
  be a drained one.
- The pool's reconciler is notified of each delete, so the pool refills on
  its other hosts straight away. The drained host is cordoned, so nothing
  is placed there.

### Why the drain persists

A drain that ran once, in the RPC, would miss VMs that reach the host, or
become unleased, after its transaction:

- A provision reserves its placement before it creates the microVM, and
  writes the VM row after. If the drain lands in between, the reservation
  stands and the row appears later on the cordoned host.
- A pre-lease hook that fails under `QUARANTINE` turns a claimed VM into a
  quarantined one.
- A `ClaimVM` that loses a `request_id` race puts its VM back to
  `AVAILABLE`.

Each of these leaves an unleased VM on the host and keeps `vm_count` above
zero. With a one-off drain, the client would have to call `DrainHost` again
until the count settles. With the drain as host state, the Sweeper picks
them up on its next tick and the client calls `DrainHost` once.

### Provisions in flight

A drain can mark a VM `DELETING` while `Provision` is still working on it.
`UpdateVM` already treats `DELETING` as terminal: its `WHERE` clause matches
a `DELETING` row only when the new phase is `DELETING` too. So the
provision's last write, to `AVAILABLE`, fails with `ErrVMDeleting`, and the
VM is never offered to a claim. `Provision` then checks the store before it
applies the hook failure policy. If the VM is `DELETING` or gone, it skips
the policy and its `VM_HOOK_FAILED` event, since no hook failed and the drain
already deletes the VM.

### Persistence

Migration 3 adds two columns to `hosts`:

```sql
ALTER TABLE hosts ADD COLUMN draining INTEGER NOT NULL DEFAULT 0;
ALTER TABLE hosts ADD COLUMN draining_at INTEGER;
```

An existing host is not draining. The `Store` interface gains `DrainHost`
and `MarkDrainingHostsVMsDeleting`, and `SetHostCordoned(false)` also ends
the drain.

## Consequences

Positive:

- A client takes a host out of service with one call, and `RemoveHost`
  succeeds once the leases on the host have ended.
- Quarantined and failed VMs on a host can be cleaned up, which covers the
  host-maintenance part of #103.
- The drain shares the delete path with `DeletePool`, and failed deletes
  are retried by the same Sweeper scan.

Negative:

- The `hosts` table gains two columns and the `Store` interface two
  methods. `NewHostAdminServer` takes a notifier.
- The Sweeper does one more store transaction per tick, and lists the hosts
  when it retries pending deletions.
- A drain keeps deleting until someone calls `UncordonHost`. A client that
  wants only to stop placements must use `CordonHost`.

## Alternatives considered

- **A `drain` field on `CordonHostRequest`.** One RPC fewer, but the
  maintainer preferred a separate RPC, and a flag that changes what an RPC
  deletes is easy to miss.
- **A one-off drain in the RPC only.** Simpler, but it misses the VMs listed
  under "Why the drain persists", and the client must poll and repeat.
- **Evict leased VMs too.** That breaks leases a client still holds. Leases
  end through release or expiry, as for a cordoned host. A force option
  could come later, as `DeletePool` has.

## Out of scope

- Forgetting the VMs of a host that is already gone. A client should drain
  before the machine goes.
- A per-VM delete RPC.
