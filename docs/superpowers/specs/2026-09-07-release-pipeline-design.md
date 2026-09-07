# Release Pipeline

Source: [liquidmetal-dev/battery#15](https://github.com/liquidmetal-dev/battery/issues/15)

## Context

`poolmgrd` (the MicroVM Warm Pool Manager server) currently has no release
process: no binaries, no container image, no tags. The proto API
(`PoolAdmin`/`Lease`/`Events`/`types`) also isn't published anywhere
consumers could depend on, and CI does no breaking-change detection against
a published schema.

Design doc [`docs/design/2026-09-05-microvm-warm-pool-manager-design.md`](../../design/2026-09-05-microvm-warm-pool-manager-design.md)
already decided: release artifacts are a container image + cross-platform
binaries via GoReleaser, triggered on tag push.

`cmd/poolmgr-hostagent` and `api/proto/poolmgr/v1alpha1/hostagent.proto`
are dead scope: issue #29 (closed) replaced the `poolmgr-hostagent` sidecar
with flintlock's native `MicroVMExec`/`MicroVMSSHProxy` services, but the
stub binary and proto were never deleted. This work deletes them as prep,
so the release pipeline only ever has one binary and the BSR module only
ever contains the four services actually meant to be published.

## Decisions

- **Binaries released**: `poolmgrd` only. `poolmgr-hostagent` is deleted,
  not released.
- **Platforms**: `linux/amd64` + `linux/arm64` only (a server daemon for
  flintlock hosts — no darwin/windows builds).
- **Container registry**: GHCR (`ghcr.io/liquidmetal-dev/poolmgrd`),
  authenticated with the built-in `GITHUB_TOKEN` — no extra secrets to
  provision.
- **Image build**: a standalone `Dockerfile` (distroless/static base) that
  `COPY`s the GoReleaser-built binary in per-arch; GoReleaser drives the
  per-arch `docker buildx build` + manifest push. No compiler in the image
  build.
- **BSR module name**: `buf.build/liquidmetal-dev/battery`.
- **BSR push timing**: only on release tags (as part of `release.yml`), not
  on every merge to `main`. Breaking-change checks on PRs compare against
  the last *released* schema, not the latest merged one.
- **Breaking-change detection**: `buf breaking --against` the BSR module,
  added to the existing `proto` job in `ci.yml`, in addition to the
  existing `buf lint`.
- **GoReleaser snapshot dry-run**: added to `ci.yml` (a new job, or folded
  into `build`) on every PR — `goreleaser release --snapshot --clean`, no
  publish — so a broken `.goreleaser.yaml`/`Dockerfile` fails CI before a
  tag is ever cut.
- **Changelog**: GoReleaser-generated, grouped by Conventional Commit
  prefix (`feat`, `fix`, `chore`, ...), matching the commit convention
  already mandated in `AGENTS.md`.

## Prep cleanup (same branch, blocking)

- Delete `cmd/poolmgr-hostagent/`.
- Delete `api/proto/poolmgr/v1alpha1/hostagent.proto` and its generated
  `hostagent.pb.go` / `hostagent_grpc.pb.go`.
- Re-run `./hack/generate-proto.sh` and commit the resulting diff (removes
  the generated hostagent files, leaves the other four services
  untouched).

## Components

### 1. `Dockerfile` (repo root)

Minimal multi-arch-friendly image:

```dockerfile
FROM gcr.io/distroless/static-debian12
COPY poolmgrd /poolmgrd
ENTRYPOINT ["/poolmgrd"]
```

GoReleaser's `dockers` section builds one image per arch from this same
Dockerfile, injecting the matching prebuilt binary via build context, then
`docker_manifests` pushes a combined multi-arch manifest.

### 2. `.goreleaser.yaml` (repo root)

- `builds`: one build (`poolmgrd`, `main: ./cmd/poolmgrd`),
  `goos: [linux]`, `goarch: [amd64, arm64]`.
- `archives`: tar.gz per platform, checksums file.
- `dockers`: two entries (amd64, arm64) targeting the Dockerfile above,
  each tagged `ghcr.io/liquidmetal-dev/poolmgrd:{{ .Tag }}-<arch>`.
- `docker_manifests`: combines the two arch images into
  `ghcr.io/liquidmetal-dev/poolmgrd:{{ .Tag }}` and `:latest`.
- `changelog`: `groups` mapping Conventional Commit prefixes to sections;
  excludes merge commits.

### 3. `.github/workflows/release.yml` (new)

- Trigger: `push: tags: ['v*.*.*']`.
- Permissions: `contents: write` (GitHub release + changelog),
  `packages: write` (GHCR push).
- Steps: checkout (`fetch-depth: 0` for changelog history) → setup-go →
  `jdx/mise-action` (installs pinned `buf`) → `docker/login-action` against
  `ghcr.io` using `GITHUB_TOKEN` → `goreleaser/goreleaser-action` (release
  mode, `--clean`) → `buf push` of `api/proto` against
  `buf.build/liquidmetal-dev/battery` using a `BUF_TOKEN` repo secret.
- `goreleaser release --clean` fails the whole job (not partially) if any
  platform build or the docker push fails — no partial releases.

### 4. `buf.yaml`

Add `name: buf.build/liquidmetal-dev/battery` at the top level so
`buf push` has a target module.

### 5. `.github/workflows/ci.yml` (extend existing `proto` job, add new job)

- `proto` job gains a step after `buf lint`:
  `buf breaking --against buf.build/liquidmetal-dev/battery`.
- New job (or step in `build`): `goreleaser release --snapshot --clean`
  (no publish, no `BUF_TOKEN`/GHCR credentials needed) on every PR and push
  to `main`, to catch `.goreleaser.yaml`/`Dockerfile` breakage before a tag
  is cut.

## Error handling / edge cases

- **First-ever tag**: `buf breaking` in `ci.yml` will fail on any PR opened
  before the first `buf push` has happened, since there's no BSR module
  version to compare against yet. The first release tag must be cut (or an
  initial manual `buf push`) before the breaking-check step is meaningful;
  note this in a comment in `ci.yml`.
- **GoReleaser snapshot job vs. real release job**: they share
  `.goreleaser.yaml` but the snapshot job must never have GHCR push
  credentials or `BUF_TOKEN` available, so a config mistake can't
  accidentally publish from a PR.

## Testing

- `goreleaser release --snapshot --clean` runs on every PR via `ci.yml` —
  primary safety net for the GoReleaser config and Dockerfile.
- Manual: cut a real tag (or run `goreleaser release --snapshot` locally)
  to confirm the GHCR multi-arch image and BSR push work end-to-end before
  relying on this in production.
