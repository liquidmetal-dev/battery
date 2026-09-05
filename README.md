# battery

[![CI](https://github.com/liquidmetal-dev/battery/actions/workflows/ci.yml/badge.svg)](https://github.com/liquidmetal-dev/battery/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/liquidmetal-dev/battery.svg)](https://pkg.go.dev/github.com/liquidmetal-dev/battery)
[![Go Report Card](https://goreportcard.com/badge/github.com/liquidmetal-dev/battery)](https://goreportcard.com/report/github.com/liquidmetal-dev/battery)
[![Go Version](https://img.shields.io/badge/go-1.25-00ADD8?logo=go)](go.mod)
[![License](https://img.shields.io/github/license/liquidmetal-dev/battery)](LICENSE)

**battery** is a MicroVM Warm Pool Manager for [flintlock](https://github.com/liquidmetal-dev/flintlock).
It manages pools of pre-provisioned, pre-booted microVMs so consumers can claim an
already-running VM instantly, instead of waiting for flintlock to provision one on demand.
It's part of the [liquidmetal-dev](https://github.com/liquidmetal-dev) family of projects,
alongside flintlock and [guest-agent](https://github.com/liquidmetal-dev/guest-agent).

> **Status:** early-stage / pre-alpha. The gRPC API surface is defined, but the pool
> reconciliation, replenishment, and lease logic is not yet implemented.

## Architecture

battery is made up of two binaries:

- **`poolmgrd`** (`cmd/poolmgrd`) — the central pool manager daemon. It serves the gRPC
  API defined under `api/proto/poolmgr/v1alpha1`:
  - `PoolAdmin` — create, update, delete, and list pool definitions.
  - `Lease` — claim a VM from a pool, heartbeat it, and release it back.
  - `Events` — a server-streaming subscription for pool/VM/lease lifecycle events.

  It reconciles a fleet of flintlock hosts against each pool's desired state, replenishing
  VMs using a per-pool strategy, and persists state to an embedded SQLite database.

- **`poolmgr-hostagent`** (`cmd/poolmgr-hostagent`) — a sidecar that runs alongside each
  flintlock host. Flintlock's guest-agent is only reachable over a host-local vsock socket,
  so this sidecar proxies `exec`/`ping` calls to the in-VM guest-agent on `poolmgrd`'s behalf,
  exposed via the `Hostagent` gRPC service.

See [`docs/design/2026-09-05-microvm-warm-pool-manager-design.md`](docs/design/2026-09-05-microvm-warm-pool-manager-design.md)
for the full design rationale and decisions.

## Getting started

### Prerequisites

- Go 1.25+
- [mise](https://mise.jdx.dev/) (recommended) to install pinned tool versions from
  `mise.toml` — [buf](https://buf.build/), golangci-lint, `protoc-gen-go`, and
  `protoc-gen-go-grpc`.

### Build and test

```sh
go build ./...
go vet ./...
go test ./...
golangci-lint run
```

### Working with the API protos

The gRPC API is defined in `api/proto` using [buf](https://buf.build/). After editing a
`.proto` file, regenerate the Go code with:

```sh
./hack/generate-proto.sh
```

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
