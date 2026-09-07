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

  It's designed to reconcile a fleet of flintlock hosts against each pool's desired state, replenishing
  VMs using a per-pool strategy, and, once implemented, persist state (e.g., in an embedded SQLite database).

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

## Releasing

Releases are cut by pushing a semver tag matching `v*.*.*` (e.g. `v0.1.0`)
to `main`. This triggers `.github/workflows/release.yml`, which:

- builds `poolmgrd` for `linux/amd64` and `linux/arm64` via GoReleaser
- publishes a multi-arch container image to
  `ghcr.io/liquidmetal-dev/poolmgrd:<version>` (and `:latest`) — note the
  image tag drops the leading `v` from the git tag (e.g. tagging `v0.1.0`
  produces `ghcr.io/liquidmetal-dev/poolmgrd:0.1.0`), unlike the GitHub
  release itself, which keeps it
- creates a GitHub release with a changelog grouped by commit type
- pushes `api/proto` (`PoolAdmin`/`Lease`/`Events`/`types`) to the Buf
  Schema Registry at `buf.build/liquidmetal-dev/battery`, creating the
  BSR module on first push if it doesn't exist yet

**One-time setup:** a `BUF_TOKEN` repository secret (a Buf Schema Registry
API token with write access to `liquidmetal-dev/battery`) must exist
before the first tag is pushed. Until the first successful `buf push`,
the `buf breaking` check in CI (`.github/workflows/ci.yml`) has nothing
to compare against; it's set to `continue-on-error` so it won't block
PRs until then.

If `buf push` fails after GoReleaser has already published successfully,
the proto schema push can be re-run manually without re-cutting the
release: `buf push --create --create-visibility public --label <tag>`
from a checkout of that tag.

```bash
git tag v0.1.0
git push origin v0.1.0
```

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.

## Acknowledgements

Thanks to [@phoban01](https://github.com/phoban01) for the original idea of using warm pools
with flintlock, based on his work building a GitLab executor that used flintlock warm pools.
