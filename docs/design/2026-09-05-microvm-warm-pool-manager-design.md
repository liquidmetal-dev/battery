# MicroVM Warm Pool Manager — Design

## Context

This project manages "warm pools" of pre-provisioned microVMs on top of
[flintlock](https://github.com/liquidmetal-dev/flintlock), so consumers can claim an
already-booted VM instantly instead of waiting for flintlock to provision one on demand.

Key grounding facts confirmed directly from the flintlock and guest-agent source (not assumed):

- Flintlock's `MicroVM` gRPC service (proto: `api/services/microvm/v1alpha1/microvms.proto`)
  exposes `CreateMicroVM`, `DeleteMicroVM`, `GetMicroVM`, `ListMicroVMs`, `ListMicroVMsStream`.
  There is **no watch/subscribe RPC** — `ListMicroVMsStream` streams the current list, it's not
  an event feed. Our pool manager must poll flintlock and derive its own state transitions/events.
- `MicroVMSpec` (proto: `api/types/microvm.proto`) has an `allow_guest_agent` bool field: when
  true, flintlock attaches a vsock device so the in-VM
  [guest-agent](https://github.com/liquidmetal-dev/guest-agent) can be reached. `MicroVMStatus`
  exposes `vsock_path` — the host-side Unix-domain-socket path for that vsock device (empty
  unless `allow_guest_agent` is set).
- The guest-agent listens on vsock with a small control protocol (exec/ping/info) and an ssh
  proxy. The host drives it via the `vsock-connect` CLI helper: `vsock-connect exec --uds
  <vsock_path> --port 1024 -- <cmd>` (exit code = command's exit code, stdout/stderr stream
  back), and `vsock-connect ping --uds <vsock_path> --port 1024` for liveness. The protocol
  internals aren't in an importable Go package (`internal/`), so the supported integration
  point is shelling out to the `vsock-connect` binary, not reimplementing the wire protocol.
- `MicroVMStatus.state` is `PENDING | CREATED | FAILED | DELETING`.
- Flintlock's own server (`internal/command/run/run.go`) supports gRPC with either TLS+mTLS-style
  cert auth or an explicit insecure mode (`cfg.TLS.Insecure`), plus optional basic auth, and
  exposes Prometheus metrics via `grpc_prometheus` + `promhttp`. We mirror this pattern.

## Decisions

- **Language**: Go.
- **Topology**: manage a fleet of multiple flintlock hosts (not just one).
- **Persistence**: embedded DB (SQLite), single active instance for v1 (no HA/leader-election yet;
  restart recovers state from disk).
- **Events**: delivered via a gRPC server-streaming subscription RPC (no webhooks/external bus for v1).
- **In-VM commands**: executed via flintlock's guest-agent vsock channel (`allow_guest_agent: true`
  is forced on for every pool-managed VM), not a custom bundled agent.
- **Replenishment**: each pool picks exactly **one** named strategy (not composable).
- **API auth**: support both mTLS and an explicit insecure (no-auth) mode, selectable in config.
- **Consumer identity**: opaque lease token returned by `ClaimVM`, presented on every
  heartbeat/release call — no tie to transport identity.
- **Heartbeat/expiry threshold**: configurable per-pool.
- **Failed create/pre-lease hook policy**: configurable per-pool — either (a) delete the VM and
  let replenishment provision a replacement, or (b) mark it unhealthy/quarantined and retain it
  for operator inspection.
- **CI/CD**: GitHub Actions.
- **Release artifacts**: container image + cross-platform binaries via GoReleaser.
- **Deployment target**: deployment-agnostic (plain binary + container image); no k8s-specific
  assumptions baked into the design for v1.
- **Guest-agent reachability across hosts**: the guest-agent's `vsock_path` is a Unix-domain
  socket local to whichever flintlock host runs the VM — not network-reachable. Since the fleet
  spans multiple flintlock hosts, we run a small **per-host sidecar** (`poolmgr-hostagent`)
  alongside each `flintlockd` that proxies guest-agent `exec`/`ping` calls over gRPC back to the
  central pool manager. This keeps the pool manager itself single-instance/host-agnostic and
  avoids requiring shared filesystem access to `/run/flintlock/*.vsock`.

## Architecture

```
                         ┌─────────────────────────────────────────┐
                         │           Pool Manager (single proc)      │
                         │                                           │
  gRPC clients  ───────► │  API Server (mTLS/insecure)               │
  (consumers,            │   ├─ PoolAdminService  (CRUD pool defs)   │
  admins)                │   ├─ LeaseService      (Claim/Heartbeat/  │
                         │   │                     Release)          │
                         │   └─ EventsService     (Subscribe stream) │
                         │                                           │
                         │  Reconciler (per-pool control loop)       │
                         │   ├─ Replenisher (strategy-driven)        │
                         │   ├─ Lease expiry sweeper                 │
                         │   └─ VM lifecycle hooks (create/pre-lease)│
                         │                                           │
                         │  Flintlock Client Pool                    │
                         │   (one gRPC client per configured host)   │
                         │                                           │
                         │  Guest-Agent Client                       │
                         │   (calls per-host hostagent over gRPC)    │
                         │                                           │
                         │  Store (SQLite): pools, vms, leases,      │
                         │                  events (outbox)          │
                         │                                           │
                         │  Metrics (Prometheus /metrics)            │
                         └──────────────┬────────────────────────────┘
                                        │ gRPC (per flintlock host)
                    ┌───────────────────┼───────────────────────┐
                    ▼                   ▼                       ▼
       ┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐
       │ Host A               │ │ Host B               │ │ Host C               │
       │ flintlockd           │ │ flintlockd           │ │ flintlockd           │
       │ poolmgr-hostagent ───┼─│ poolmgr-hostagent ───┼─│ poolmgr-hostagent    │
       │  (proxies vsock-     │ │  (proxies vsock-     │ │  (proxies vsock-     │
       │   connect exec/ping) │ │   connect exec/ping) │ │   connect exec/ping) │
       └────────────────────┘ └────────────────────┘ └────────────────────┘
```

### Components

- **API Server** — gRPC server implementing three services (below). TLS mode (mTLS or
  insecure) is a startup config choice, matching flintlock's own `--tls-insecure` pattern.
- **Reconciler** — one control loop per pool (goroutine), driven by a ticker plus event
  triggers (VM claimed, VM deleted). Responsible for: comparing desired vs actual pool state,
  invoking the configured replenishment strategy, running create/pre-lease hooks on new VMs
  before marking them `Available`, and sweeping expired leases.
- **Flintlock Client Pool** — holds a gRPC client per configured flintlock host (from static
  config or a discovery list); the reconciler picks a host for each new VM using a simple
  placement policy (see Scheduling).
- **poolmgr-hostagent** (new small component, one instance per flintlock host) — a thin gRPC
  server, colocated with `flintlockd`, that receives `WaitReady(vsock_path)` /
  `Run(vsock_path, cmd)` calls from the pool manager and executes them locally by shelling out
  to `vsock-connect` (`ping` / `exec --uds <vsock_path> --port 1024`). This is the only
  component that needs local filesystem access to `/run/flintlock/*.vsock`.
- **Guest-Agent Client** (in the pool manager) — calls the appropriate host's
  `poolmgr-hostagent` (matched via the VM's `flintlock_host`) for `WaitReady`/`Run`. Used for
  both the creation hook and the pre-lease hook.
- **Store** — SQLite via a small repository layer; also holds an events "outbox" table so
  `EventsService.Subscribe` can replay recent events to a newly-connected subscriber and so
  event emission survives a crash between "decided to emit" and "delivered."
- **Metrics** — `/metrics` HTTP endpoint (Prometheus format), instrumented at the reconciler
  and API layer (see Metrics section).

### Multi-host fleet & scheduling (v1 scope)

Flintlock hosts are static config entries (address + TLS materials) grouped implicitly by
whatever the pool's spec requires (resource capacity is not tracked in v1 beyond a simple
round-robin/least-loaded-by-VM-count placement across the hosts eligible for a pool). Each pool
definition lists which flintlock host(s) it's allowed to place VMs on. A pool's VMs can span
multiple hosts. No cross-pool bin-packing or live host capacity probing in v1 — this keeps
scheduling simple and is an explicit place to extend later (e.g. querying host resource usage)
without changing the external API.

## Data Model

**PoolSpec** (persisted; admin-managed):
- `name`, `namespace`
- `microvm_template`: a `flintlock.types.MicroVMSpec` template (vcpu, memory, kernel, volumes,
  interfaces, labels, metadata) — `allow_guest_agent` is always forced `true` server-side.
- `size`: target pool size
- `flintlock_hosts`: list of eligible host identifiers
- `replenishment_strategy`: enum `IMMEDIATE_ON_LEASE | MIN_SIZE_THRESHOLD | REPLACE_ON_DELETE`
  (+ strategy-specific params, e.g. `min_size` threshold)
- `create_commands`: []string — run once after boot + guest-agent ready
- `pre_lease_commands`: []string — run immediately before a claim is handed to a consumer
- `hook_failure_policy`: enum `DELETE_AND_REPLACE | QUARANTINE`
- `heartbeat_interval`, `heartbeat_expiry_threshold` (durations)

**VMRecord** (per-VM state owned by the pool manager, cross-referenced with flintlock's own
`MicroVMStatus` by UID):
- `uid` (flintlock UID), `pool_name`, `flintlock_host`
- `phase`: `PROVISIONING → CREATE_HOOK_RUNNING → AVAILABLE → LEASED → PRE_LEASE_HOOK_RUNNING
  (transient, before handing to consumer) → DELETING` / `QUARANTINED` / `FAILED`
- `lease_id` (nullable), timestamps

**Lease**:
- `lease_id` (opaque token, returned from `ClaimVM`), `vm_uid`, `pool_name`
- `claimed_at`, `last_heartbeat_at`, `expires_at` (computed from pool's threshold)

**Event** (outbox row): `id`, `pool_name`, `vm_uid`, `type`, `payload`, `created_at`.
Event types: `VMProvisioned`, `VMAvailable`, `VMClaimed`, `VMReleased`,
`VMExpiringSoon` (optional pre-warning), `VMDeletedDueToExpiry`, `VMDeletedOnRelease`,
`VMHookFailed`, `PoolReplenishing`, `PoolSizeBelowTarget`.

## gRPC API (sketch)

```proto
service PoolAdmin {
  rpc CreatePool(CreatePoolRequest) returns (Pool);
  rpc UpdatePool(UpdatePoolRequest) returns (Pool);
  rpc DeletePool(DeletePoolRequest) returns (google.protobuf.Empty);
  rpc GetPool(GetPoolRequest) returns (Pool);
  rpc ListPools(ListPoolsRequest) returns (ListPoolsResponse);
}

service Lease {
  rpc ClaimVM(ClaimVMRequest) returns (ClaimVMResponse);       // returns lease_id + VM connection info
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse); // keyed by lease_id
  rpc ReleaseVM(ReleaseVMRequest) returns (google.protobuf.Empty);
}

service Events {
  rpc Subscribe(SubscribeRequest) returns (stream Event); // optional pool_name filter
}
```

`ClaimVMResponse` returns enough for the consumer to reach the VM (IP/interfaces from flintlock's
`MicroVMStatus.network_interfaces`) plus the `lease_id`. `ClaimVM` fails with a distinct status
(e.g. `RESOURCE_EXHAUSTED`) when no VM is `AVAILABLE`.

## Lease Lifecycle

1. Consumer calls `ClaimVM(pool_name)`. Manager atomically picks an `AVAILABLE` VM, runs the
   pool's `pre_lease_commands` via the guest-agent client (state → `PRE_LEASE_HOOK_RUNNING`),
   then transitions to `LEASED`, creates a `Lease` row, emits `VMClaimed`, decrements
   available count.
   - If `IMMEDIATE_ON_LEASE` strategy: reconciler is nudged immediately to provision one
     replacement VM.
2. Consumer periodically calls `Heartbeat(lease_id)`; updates `last_heartbeat_at`/`expires_at`.
3. Release happens via:
   - explicit `ReleaseVM(lease_id)` call, or
   - the expiry sweeper finding `now > expires_at` with no recent heartbeat.
   Either path: emit `VMExpiringSoon` (sweeper path, fired once, before the deadline — window
   configurable) → then on actual expiry/release, delete the VM via flintlock, emit
   `VMDeletedOnRelease`/`VMDeletedOnExpiry`, delete the `Lease` row.
   - If `REPLACE_ON_DELETE` strategy: reconciler provisions a replacement immediately.

## Replenishment Strategies

- `IMMEDIATE_ON_LEASE`: on every successful claim, immediately start provisioning one new VM.
- `MIN_SIZE_THRESHOLD`: reconciler loop checks available count against `min_size`; when below,
  provisions up to `size`.
- `REPLACE_ON_DELETE`: on every VM deletion (expiry, release-triggered, or hook failure), start
  provisioning exactly one replacement.

All strategies share the same provisioning pipeline: `CreateMicroVM` (flintlock) → poll
`GetMicroVM` until `CREATED` with a non-empty `vsock_path` → guest-agent `WaitReady` → run
`create_commands` → on success mark `AVAILABLE` + emit `VMAvailable`; on failure apply the
pool's `hook_failure_policy`.

## Metrics (Prometheus, `/metrics`)

Per-pool gauges/counters (labeled by `pool_name`):
- `poolmgr_pool_size` (target), `poolmgr_pool_available`, `poolmgr_pool_leased`,
  `poolmgr_pool_provisioning`, `poolmgr_pool_quarantined`
- `poolmgr_vm_claims_total`, `poolmgr_vm_releases_total{reason="api|expiry"}`
- `poolmgr_vm_provision_duration_seconds` (histogram), `poolmgr_hook_duration_seconds{hook="create|pre_lease"}`
- `poolmgr_hook_failures_total{hook,pool_name}`
- `poolmgr_lease_duration_seconds` (histogram)

Plus standard gRPC server metrics via `grpc_prometheus` interceptors, matching flintlock's own
convention.

## Storage Schema (SQLite, outline)

Tables: `pools`, `vms`, `leases`, `events`. Reconciler and API handlers go through a repository
interface (`internal/store`) so SQLite can later be swapped without touching business logic.
`events` table doubles as the outbox for `Events.Subscribe` (new subscribers get recent
unacked rows replayed, keyed by a monotonic id per pool).

## Repo Layout (target shape)

```
/cmd/poolmgrd              — server entrypoint
/cmd/poolmgr-hostagent      — per-flintlock-host sidecar entrypoint
/api/proto/poolmgr/v1alpha1 — our own .proto definitions (PoolAdmin/Lease/Events/hostagent)
/internal/reconciler        — per-pool control loop, replenishment strategies
/internal/flintlockclient   — thin wrapper over generated flintlock gRPC client, one per host
/internal/guestagent        — hostagent gRPC client (WaitReady/Run), used by pool manager
/internal/hostagent         — hostagent gRPC server impl (wraps vsock-connect locally)
/internal/store             — SQLite repository (pools, vms, leases, events)
/internal/api               — gRPC service implementations
/internal/config            — config loading, TLS setup (mirrors flintlock's cmdflags pattern)
/deploy                     — Dockerfile, example k8s manifests (non-binding)
/.github/workflows          — ci.yml (lint/test/build), release.yml (goreleaser on tag)
/.goreleaser.yaml
```

## Testing/Verification Approach

- Unit tests per package (reconciler strategies, lease expiry logic, store repository) using
  Go's standard testing + an in-memory/temp-file SQLite DB.
- A fake flintlock gRPC server (implementing the same `MicroVM` service interface) for
  reconciler integration tests, so tests don't require a real flintlock/Firecracker host.
- A fake guest-agent client interface for hook-running tests (no real vsock needed).
- CI (`ci.yml`): `go build`, `go vet`, `golangci-lint`, `go test ./...` on PRs.
- Manual/E2E verification (documented, not automated in v1): run against a real flintlockd +
  Firecracker VM with `allow_guest_agent: true` and confirm `vsock-connect ping` succeeds,
  confirm claim/heartbeat/release/expiry flow end-to-end, confirm `/metrics` and
  `Events.Subscribe` reflect real transitions.

## Open Items Deferred Past v1 (explicitly out of scope)

- HA/multi-replica pool manager (leader election or shared DB).
- Cross-pool bin-packing / real host resource-aware scheduling.
- Webhook/external event sinks (only gRPC streaming subscription in v1).
- Kubernetes-native packaging (CRDs/operator) — plain binary/container only.
