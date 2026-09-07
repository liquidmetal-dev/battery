# End-to-End Manual Verification Runbook

This runbook is the manual/E2E counterpart to the automated test suite (`go test ./...`, which
runs against a fake flintlock gRPC server — see
[`docs/design/2026-09-05-microvm-warm-pool-manager-design.md`](../design/2026-09-05-microvm-warm-pool-manager-design.md#testingverification-approach)).
It walks through verifying `battery` against a **real** `flintlockd` v0.13.0+ and Firecracker VM.

It supersedes the design doc's original `vsock-connect ping` step: flintlock v0.13.0 added native
`MicroVMExec` and `MicroVMSSHProxy` gRPC services served directly by `flintlockd`, which replaced
the `poolmgr-hostagent`/vsock-connect path (see [#29](https://github.com/liquidmetal-dev/battery/issues/29)).
`internal/flintlockclient` (`exec.go`, `pool.go`) talks to these directly.

## Status of this runbook

- **Runnable today**: flintlockd's `MicroVMExec`/`MicroVMSSHProxy` (steps 2–4), `poolmgrd`'s
  `/metrics` startup check and `PoolAdmin` CRUD (steps 5–6), and an `Events.Subscribe`
  connectivity check (step 7).
- **Blocked on [#40](https://github.com/liquidmetal-dev/battery/issues/40)** ("Dynamic per-pool
  Reconciler lifecycle"): nothing starts a `Reconciler` per pool yet, so `CreatePool` never
  provisions a VM or marks one `AVAILABLE`. `ClaimVM` therefore always fails
  `RESOURCE_EXHAUSTED`, and `Heartbeat`/`ReleaseVM`/lease-expiry/replenishment can't be exercised
  through the real API. Step 8 documents the commands to run once that lands.

## Prerequisites

- A real Firecracker-capable host running `flintlockd` v0.13.0+. Follow flintlock's own
  getting-started guides for the underlying infra — this runbook doesn't duplicate them:
  - [Firecracker setup](https://github.com/liquidmetal-dev/flintlock/blob/main/userdocs/docs/getting-started/firecracker.md)
  - [containerd setup](https://github.com/liquidmetal-dev/flintlock/blob/main/userdocs/docs/getting-started/containerd.md)
  - [Network setup](https://github.com/liquidmetal-dev/flintlock/blob/main/userdocs/docs/getting-started/network.md)
- [`grpcurl`](https://github.com/fullstorydev/grpcurl): `go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest`
- Go 1.25+ (to build/run `poolmgrd` and the small SSH-proxy bridge script in step 5)
- An `ssh` client
- Optionally `sqlite3`, for inspecting `poolmgrd`'s database while debugging

Both `flintlockd` and `poolmgrd` register gRPC server reflection, so `grpcurl` doesn't need
`.proto` files on disk — just `-plaintext <addr> <service>/<method>`.

## 1. Start flintlockd with exec and SSH-proxy enabled

```sh
flintlockd run --insecure --enable-exec-api --enable-ssh-proxy-api --parent-iface <host-interface>
```

`--enable-exec-api`/`--enable-ssh-proxy-api` gate the `MicroVMExec`/`MicroVMSSHProxy` gRPC
services (both default to off: exec runs arbitrary commands in a guest, and SSH-proxying tunnels
a client straight to the guest's `sshd`). `--insecure` matches `battery`'s own
`TLSConfig.Insecure`/`ServerTLSConfig.Insecure` for this runbook; for a production-shaped check,
use flintlock's mTLS flags and `battery`'s `CertFile`/`KeyFile`/`CAFile` config instead.

`--parent-iface <host-interface>` (or `--bridge-name <bridge>` if you're using a bridge instead —
see the [Network setup](https://github.com/liquidmetal-dev/flintlock/blob/main/userdocs/docs/getting-started/network.md)
prerequisite above) is required: flintlockd refuses to start unless at least one of the two is
set, so use whichever the network setup step left you with.

## 2. Create a real MicroVM

Save a `CreateMicroVM` payload (adapted from flintlock's own
[`hack/scripts/payload/CreateMicroVM.json`](https://github.com/liquidmetal-dev/flintlock/blob/main/hack/scripts/payload/CreateMicroVM.json),
with `allow_guest_agent` added — required for both `MicroVMExec` and `MicroVMSSHProxy`):

```json
{
  "microvm": {
    "id": "e2e-check",
    "namespace": "e2e",
    "vcpu": 2,
    "memory_in_mb": 2048,
    "kernel": {
      "image": "docker.io/richardcase/ubuntu-bionic-kernel:0.0.11",
      "filename": "vmlinux",
      "add_network_config": true
    },
    "initrd": {
      "image": "docker.io/richardcase/ubuntu-bionic-kernel:0.0.11",
      "filename": "initrd-generic"
    },
    "rootVolume": {
      "id": "root",
      "is_read_only": false,
      "source": { "container_source": "docker.io/richardcase/ubuntu-bionic-test:cloudimage_v0.0.1" }
    },
    "interfaces": [
      { "device_id": "eth1", "type": 1, "address": { "address": "192.168.100.30/32" } }
    ],
    "allow_guest_agent": true
  }
}
```

```sh
grpcurl -d @ -plaintext localhost:9090 \
  microvm.services.api.v1alpha1.MicroVM/CreateMicroVM \
  < create-microvm.json
```

Poll until it's up:

```sh
grpcurl -d '{"uid": "<uid-from-create-response>"}' -plaintext localhost:9090 \
  microvm.services.api.v1alpha1.MicroVM/GetMicroVM
```

Wait for `"state": "CREATED"`. Note the `uid` — every step below needs it.

## 3. Verify `MicroVMExec.ExecCommand` end-to-end

This is the step that replaces the old `vsock-connect ping` check. `MicroVMExec.ExecCommand` is a
bidirectional stream; a hook-style call (matching what `flintlockclient.Exec` sends, with
`has_stdin: false`) is a single client message followed by half-closing the stream:

```sh
echo '{"start": {"uid": "<uid>", "cmd": "uname -a", "shell": true}}' | \
  grpcurl -d @ -plaintext localhost:9090 \
  microvmexec.services.api.v1alpha1.MicroVMExec/ExecCommand
```

Expect a `stdout` message with the command's output, followed by a terminal `exit_code: 0`. A
non-zero `exit_code` is a normal result (the command ran); an `error` field or an RPC failure
means the guest-agent path itself is broken — check that `allow_guest_agent` was set at create
time, the VM is `CREATED`, and `flintlockd` was started with `--enable-exec-api`.

## 4. Verify `MicroVMSSHProxy.SSHProxy` end-to-end

`MicroVMSSHProxy.SSHProxy` tunnels raw bytes to the guest's `sshd` — it does no authentication of
its own. The most faithful manual check is a real interactive SSH session, using a small bridge
program as an `ssh` `ProxyCommand`.

Save as `sshproxy_bridge.go`:

```go
// Command sshproxy_bridge bridges stdin/stdout to flintlockd's
// MicroVMSSHProxy.SSHProxy for the given microvm uid, for use as an
// `ssh -o ProxyCommand` target. Not part of the battery module — a
// throwaway script for this runbook only.
package main

import (
	"context"
	"io"
	"log"
	"os"

	microvmsshproxyv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmsshproxy/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("usage: %s <flintlockd-addr> <microvm-uid>", os.Args[0])
	}
	addr, uid := os.Args[1], os.Args[2]

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := microvmsshproxyv1alpha1.NewMicroVMSSHProxyClient(conn).SSHProxy(context.Background())
	if err != nil {
		log.Fatalf("open stream: %v", err)
	}

	start := &microvmsshproxyv1alpha1.SSHProxyRequest{
		Payload: &microvmsshproxyv1alpha1.SSHProxyRequest_Uid{Uid: uid},
	}
	if err := stream.Send(start); err != nil {
		log.Fatalf("send uid: %v", err)
	}

	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := os.Stdin.Read(buf)
			if n > 0 {
				req := &microvmsshproxyv1alpha1.SSHProxyRequest{
					Payload: &microvmsshproxyv1alpha1.SSHProxyRequest_Data{Data: buf[:n]},
				}
				if sendErr := stream.Send(req); sendErr != nil {
					log.Fatalf("send data: %v", sendErr)
				}
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("recv: %v", err)
		}
		os.Stdout.Write(resp.GetData())
	}
}
```

Run it as an SSH `ProxyCommand`:

```sh
ssh -o ProxyCommand="go run sshproxy_bridge.go localhost:9090 <uid>" root@e2e-check
```

A working login (or at least an SSH banner/auth prompt instead of a hang or connection error)
confirms the tunnel path end-to-end. As with exec, an error here first means checking
`allow_guest_agent`, VM state, and `--enable-ssh-proxy-api`.

## 5. Start poolmgrd against this flintlockd

Example config (`poolmgrd-config.json`):

```json
{
  "hosts": [
    { "name": "host-a", "address": "localhost:9090", "tls": { "insecure": true } }
  ],
  "api_server": {
    "addr": ":9091",
    "tls": { "insecure": true }
  },
  "metrics_addr": ":9092"
}
```

```sh
go run ./cmd/poolmgrd -config poolmgrd-config.json -db /tmp/poolmgr-e2e.db
```

Confirm `/metrics` is up. With no pool created yet, `poolmgr_pool_*` won't appear —
`internal/metrics/pool_collector.go`'s `Collect` only emits a pool's gauges once it exists in the
store — so check the gRPC server metrics instead, which `poolmgrd` pre-initializes (zero-valued)
for every registered RPC method at startup:

```sh
curl -s localhost:9092/metrics | grep '^grpc_server_started_total'
```

## 6. `PoolAdmin` CRUD verification

This uses a real, provisionable `microvm_template` (same shape as step 2's `CreateMicroVM`
payload) rather than a token one — flintlock validates `memory_in_mb >= 1024` and requires a root
volume plus at least one network interface, so a minimal `{vcpu, memory_in_mb}` template would
never let a VM reach `AVAILABLE` once [#40](https://github.com/liquidmetal-dev/battery/issues/40)
starts provisioning against it:

```sh
grpcurl -d '{
  "spec": {
    "name": "e2e-pool", "namespace": "e2e",
    "microvm_template": {
      "vcpu": 2,
      "memory_in_mb": 2048,
      "kernel": {
        "image": "docker.io/richardcase/ubuntu-bionic-kernel:0.0.11",
        "filename": "vmlinux",
        "add_network_config": true
      },
      "initrd": {
        "image": "docker.io/richardcase/ubuntu-bionic-kernel:0.0.11",
        "filename": "initrd-generic"
      },
      "root_volume": {
        "id": "root",
        "is_read_only": false,
        "source": { "container_source": "docker.io/richardcase/ubuntu-bionic-test:cloudimage_v0.0.1" }
      },
      "interfaces": [
        { "device_id": "eth1", "type": 1, "address": { "address": "192.168.100.31/32" } }
      ]
    },
    "size": 1,
    "flintlock_hosts": ["host-a"],
    "replenishment_strategy": { "type": "MIN_SIZE_THRESHOLD", "min_size": 1 },
    "hook_failure_policy": "DELETE_AND_REPLACE"
  }
}' -plaintext localhost:9091 poolmgr.v1alpha1.PoolAdmin/CreatePool

grpcurl -d '{"ref": {"name": "e2e-pool", "namespace": "e2e"}}' \
  -plaintext localhost:9091 poolmgr.v1alpha1.PoolAdmin/GetPool

grpcurl -d '{"namespace": "e2e"}' -plaintext localhost:9091 poolmgr.v1alpha1.PoolAdmin/ListPools
```

`GetPool`/`ListPools` should return the spec with a `status` object whose counts are all `0` —
that's expected today (see [Status of this runbook](#status-of-this-runbook)), not a bug: nothing
provisions VMs against a pool until #40 starts a `Reconciler` for it. Now that a pool exists, its
`poolmgr_pool_*` gauges should also appear:

```sh
curl -s localhost:9092/metrics | grep '^poolmgr_pool_'
# poolmgr_pool_size{pool_name="e2e-pool",pool_namespace="e2e"} 1
# poolmgr_pool_available{pool_name="e2e-pool",pool_namespace="e2e"} 0
# poolmgr_pool_leased{pool_name="e2e-pool",pool_namespace="e2e"} 0
# poolmgr_pool_provisioning{pool_name="e2e-pool",pool_namespace="e2e"} 0
# poolmgr_pool_quarantined{pool_name="e2e-pool",pool_namespace="e2e"} 0
```

Leave `e2e-pool` in place — step 8 reuses it once #40 lands. (If you're not continuing to step 8
right now, clean it up with `DeletePool`:
`grpcurl -d '{"ref": {"name": "e2e-pool", "namespace": "e2e"}}' -plaintext localhost:9091 poolmgr.v1alpha1.PoolAdmin/DeletePool`.)

## 7. `Events.Subscribe` connectivity check

In one terminal:

```sh
grpcurl -d '{}' -plaintext localhost:9091 poolmgr.v1alpha1.Events/Subscribe
```

In another, poke the store with a throwaway pool (using a different name so `e2e-pool` from step 6
is left untouched — step 8 needs it):

```sh
grpcurl -d '{"spec": {"name": "e2e-events-poke", "namespace": "e2e", "microvm_template": {"vcpu": 1, "memory_in_mb": 1024, "root_volume": {"id": "root", "is_read_only": false, "source": {"container_source": "docker.io/richardcase/ubuntu-bionic-test:cloudimage_v0.0.1"}}, "interfaces": [{"device_id": "eth1", "type": 1, "address": {"address": "192.168.100.32/32"}}]}, "size": 0, "flintlock_hosts": ["host-a"], "replenishment_strategy": {"type": "MIN_SIZE_THRESHOLD", "min_size": 0}, "hook_failure_policy": "DELETE_AND_REPLACE"}}' \
  -plaintext localhost:9091 poolmgr.v1alpha1.PoolAdmin/CreatePool

grpcurl -d '{"ref": {"name": "e2e-events-poke", "namespace": "e2e"}}' \
  -plaintext localhost:9091 poolmgr.v1alpha1.PoolAdmin/DeletePool
```

Confirm the stream stays open and doesn't error. No `Event` message is expected yet: every
`EventType` in `api/proto/poolmgr/v1alpha1/types.proto` originates from a successful
`ClaimVM`/`ReleaseVM` or from reconciler-driven provisioning, neither of which run without #40 —
this step only proves the subscription itself works.

## 8. Blocked: claim / heartbeat / release / expiry

Blocked on [#40](https://github.com/liquidmetal-dev/battery/issues/40). Once a `Reconciler` is
started per pool and a `CreatePool` call actually provisions VMs, come back and run these against
`e2e-pool` from step 6 — its `microvm_template` is a real, provisionable spec (unlike a token
`{vcpu, memory_in_mb}` template, it'll actually pass flintlock's create validation and reach
`AVAILABLE`):

```sh
# Expect a real lease_id + vm_uid once a VM is AVAILABLE (RESOURCE_EXHAUSTED until then).
grpcurl -d '{"pool": {"name": "e2e-pool", "namespace": "e2e"}}' \
  -plaintext localhost:9091 poolmgr.v1alpha1.Lease/ClaimVM

grpcurl -d '{"lease_id": "<lease_id>"}' \
  -plaintext localhost:9091 poolmgr.v1alpha1.Lease/Heartbeat

grpcurl -d '{"lease_id": "<lease_id>"}' \
  -plaintext localhost:9091 poolmgr.v1alpha1.Lease/ReleaseVM
```

While the `Events.Subscribe` stream from step 7 is open, confirm the expected sequence appears:
`VM_PROVISIONED → VM_AVAILABLE → VM_CLAIMED → ... → VM_DELETED_ON_RELEASE` (or
`VM_DELETED_DUE_TO_EXPIRY` if the lease is left to expire instead of released explicitly), plus
`POOL_REPLENISHING`/`POOL_SIZE_BELOW_TARGET` around replenishment.

## Troubleshooting

- **`ClaimVM` returns `RESOURCE_EXHAUSTED`**: expected today — see
  [#40](https://github.com/liquidmetal-dev/battery/issues/40). Not a bug until that lands.
- **`ExecCommand`/`SSHProxy` errors or hangs**: check, in order — was the VM created with
  `"allow_guest_agent": true`? Is `GetMicroVM` reporting `state: CREATED`? Was `flintlockd`
  started with `--enable-exec-api`/`--enable-ssh-proxy-api`?
- **`grpcurl` fails to resolve a service/method**: both `flintlockd` and `poolmgrd` register gRPC
  reflection, so this usually means a transport mismatch — check the server's TLS mode
  (`--insecure` vs. mTLS) matches how `grpcurl` is being invoked (`-plaintext` vs. `-cacert`/`-cert`/`-key`).
