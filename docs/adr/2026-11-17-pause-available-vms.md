# ADR: Pause a warm VM while it waits in AVAILABLE

Related: [issue #165](https://github.com/liquidmetal-dev/battery/issues/165),
[issue #129](https://github.com/liquidmetal-dev/battery/issues/129),
[flintlock#1330](https://github.com/liquidmetal-dev/flintlock/issues/1330).

- Status: Accepted
- Date: 2026-11-17

## Context

A warm VM waits in `AVAILABLE` until a claim takes it. Its guest keeps
running whatever its create hooks started. A kind guest runs kubelet, etcd
and its controllers, so a pool of idle warm VMs takes host CPU from the VMs
that do work.

flintlock now has `PauseMicroVM` and `ResumeMicroVM`. A pause stops the
vCPUs and keeps the memory and devices. The state becomes `PAUSED`, and a
resume puts it back to `CREATED`. Pausing a paused VM and resuming a
running one both succeed. Other states fail with `FAILED_PRECONDITION`. A
paused VM stays paused across a flintlockd restart, and a delete works on
it. The guest agent does not answer while the VM is paused, so flintlock's
exec and SSH proxy refuse a paused VM.

## Decision

### The option

`PoolSpec` gets `bool pause_when_available = 12`. The default is false, so
existing pools do not change. The store keeps it in a new
`pools.pause_when_available` column (migration 4).

`VMRecord` gets `bool paused = 9`, kept in a new `vms.paused` column in the
same migration. It is true while battery has paused the VM and not resumed
it.

### Provision

After the create hooks pass, `Provision` calls `PauseMicroVM` if the pool
has the option. The same store write that marks the VM `AVAILABLE` sets
`paused`. The pause comes last because the readiness probe and the create
hooks need the guest agent.

A failed pause gets the hook failure policy, as a failed create hook does.
`Provision` returns `ErrPauseFailed`, and the failure counts in
`poolmgr_hook_failures_total{hook="create"}`.

If poolmgrd stops between the pause and the store write, the VM is still
`CREATE_HOOK_RUNNING`. `RecoverAbandonedProvisions` deletes it at the next
start, and a delete works on a paused VM.

### ClaimVM

`runPreLeaseHooks` moves the VM to `PRE_LEASE_HOOK_RUNNING` as before.
Then, if `vm.paused` is set, it calls `ResumeMicroVM` and clears the flag.
The pre-lease hooks run after that. `LeaseVM` persists the cleared flag
with the lease.

A failed resume gets the hook failure policy, as a failed pre-lease hook
does. The claim fails with `INTERNAL`, and the failure counts in
`poolmgr_hook_failures_total{hook="pre_lease"}`. A failed pre-lease hook
does not make the claim take another VM today, so a failed resume does not
either. The client retries the claim, as it does after a failed hook.

There is no readiness check after the resume. The guest agent was ready
before the pause, and a resume only starts the vCPUs again. A probe would
add at least one exec round trip to every claim. A pre-lease hook that
can't reach the guest agent fails, and gets the policy, as on any VM.

A claim of a paused VM costs one extra RPC to the flintlock host. The new
histogram `poolmgr_vm_power_change_duration_seconds{change="pause|resume"}`
measures it. A claim of a VM that is not paused makes no extra call.

### The rule for UpdatePool

A change to `pause_when_available` affects only the VMs that `Provision`
makes after the change. Existing VMs keep their state:

- Turned on: the running `AVAILABLE` VMs stay running until a claim takes
  them or something replaces them.
- Turned off: the paused `AVAILABLE` VMs stay paused. A claim still resumes
  them, because `ClaimVM` reads `vm.paused`, not the pool's option.

### Checks that reach the guest agent

No code reaches the guest agent of an `AVAILABLE` VM. The Sweeper works on
leases, `DELETING` VMs and draining hosts, and only deletes. The only reads
of flintlock's state are `waitCreated`, before the pause, and the network
interfaces in the claim response, after the resume. So nothing needs to
skip a paused VM, and nothing reads `PAUSED` as a failure.

### Deletes

`DeletePool`, a host drain, `ReleaseVM`, the hook failure policy and the
Sweeper all delete through `DeleteMicroVM`. flintlock deletes a paused VM,
so they need no change.

## Consequences

Positive:

- A pool of warm VMs uses no host CPU while it waits.
- A pool that does not set the option behaves as before, and makes no new
  calls.
- `vm.paused` records what battery did to the VM. #129 can set and clear
  the same flag for a leased VM.

Negative:

- A claim of a paused VM waits for one more RPC. We expect milliseconds.
  We still have to measure it on a real host.
- A paused VM still holds its memory on the host.
- The pool's hosts need a flintlock that has `PauseMicroVM`. On an older
  flintlockd the pause fails with `UNIMPLEMENTED`, every new VM gets the
  hook failure policy, and the pool never fills. battery does not check
  this when the pool is created, because no flintlock release has the call
  yet, so there is no version to check against.
- A claim that loses a `request_id` race puts its VM back to `AVAILABLE`
  after it resumed it. That VM waits running, not paused, until a claim
  takes it. This race is rare, and the VM is correct, only not paused.

## Alternatives considered

- **Resume on every claim from a pool with the option.** No new VM field.
  But a VM paused before the option was turned off would never be resumed,
  and its consumer could not reach it.
- **Resume on every claim.** Simple and correct, because a resume of a
  running VM succeeds. But every claim, in every pool, pays one more RPC,
  and fails on a flintlockd without the call.
- **Read the state from flintlock with `GetMicroVM` on each claim.** It
  is always right, but it is one more RPC on every claim, and a resume still
  follows it.
- **Pause or resume the existing VMs when the option changes.** It makes
  the pool match its spec at once. But it adds a new walk over the pool's
  VMs, which can race the claims that are running, for a state that
  replacement reaches anyway.
- **Pause with a reconciler loop over `AVAILABLE` VMs.** It would catch a
  VM that a yielded claim left running. It is more code, and it adds work on
  every reconcile, for a rare case.

## Out of scope

- Pausing a leased VM that goes idle, and resuming it on the next exec or
  heartbeat (#129).
- Snapshots, and stopping a VM.
