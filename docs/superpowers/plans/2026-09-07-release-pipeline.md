# Release Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `poolmgrd` a tag-triggered release pipeline (cross-platform binaries + multi-arch GHCR container image via GoReleaser) and publish the `PoolAdmin`/`Lease`/`Events`/`types` protos to the Buf Schema Registry, with breaking-change detection in CI.

**Architecture:** A `.goreleaser.yaml` drives binary builds (`linux/amd64`, `linux/arm64`) and multi-arch Docker image builds/pushes from a minimal distroless `Dockerfile`. `.github/workflows/release.yml` runs GoReleaser and `buf push` on `v*.*.*` tag pushes. `.github/workflows/ci.yml` gains a `buf breaking` check and a `goreleaser --snapshot` dry-run so config errors surface on every PR, not just at tag time. Dead scope (`cmd/poolmgr-hostagent`, `hostagent.proto`) is deleted first so the release only ever ships one binary and the BSR module only ever contains the four live services.

**Tech Stack:** Go 1.25, GoReleaser v2, Buf CLI 1.72.0 (pinned in `mise.toml`), GitHub Actions, GHCR, Buf Schema Registry.

**Spec:** `docs/superpowers/specs/2026-09-07-release-pipeline-design.md`

## Global Constraints

- Platforms: `linux/amd64` + `linux/arm64` only — no darwin/windows builds.
- Container image: `ghcr.io/liquidmetal-dev/poolmgrd`, authenticated with `GITHUB_TOKEN` (no extra registry secrets).
- BSR module: `buf.build/liquidmetal-dev/battery`.
- BSR push happens only on release tags (in `release.yml`), never on plain merges to `main`.
- Commit messages follow Conventional Commits (`AGENTS.md`) — GoReleaser's changelog groups depend on this.
- GitHub Actions are pinned by commit SHA with a `# vX.Y.Z` comment, matching the existing style in `.github/workflows/ci.yml`.
- No agent footers in commit messages or PR descriptions (`AGENTS.md`).

---

### Task 1: Delete dead `poolmgr-hostagent` scope

Removes the stub binary and proto that issue #29 already made obsolete, so later tasks don't have to special-case them.

**Files:**
- Delete: `cmd/poolmgr-hostagent/main.go` (and the now-empty `cmd/poolmgr-hostagent/` directory)
- Delete: `api/proto/poolmgr/v1alpha1/hostagent.proto`
- Delete (generated, will be removed by regeneration): `api/proto/poolmgr/v1alpha1/hostagent.pb.go`, `api/proto/poolmgr/v1alpha1/hostagent_grpc.pb.go`

**Interfaces:**
- Produces: a repo where `api/proto/poolmgr/v1alpha1/` contains only `events`, `lease`, `pooladmin`, `types` proto/generated files, and `cmd/` contains only `poolmgrd`. Later tasks (BSR push, GoReleaser build list) rely on this being true.

- [ ] **Step 1: Delete the hostagent proto source**

```bash
git rm api/proto/poolmgr/v1alpha1/hostagent.proto
```

- [ ] **Step 2: Regenerate proto code to drop the generated hostagent files**

```bash
./hack/generate-proto.sh
git status --short api/proto
```

Expected: `hostagent.pb.go` and `hostagent_grpc.pb.go` show as deleted (`D`); no other files under `api/proto` change.

- [ ] **Step 3: Delete the poolmgr-hostagent command stub**

```bash
git rm -r cmd/poolmgr-hostagent
```

- [ ] **Step 4: Verify the build still succeeds without the deleted package**

```bash
go build ./...
go vet ./...
```

Expected: both succeed with no errors (nothing else in the repo references `cmd/poolmgr-hostagent` or the hostagent proto package — confirm with the grep below if either command fails).

```bash
grep -rl "poolmgr-hostagent\|hostagent\." --include="*.go" . || echo "no references found"
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: remove dead poolmgr-hostagent scope

poolmgr-hostagent was superseded by flintlock's native MicroVMExec/
MicroVMSSHProxy services (see #29) but the stub binary and its proto
were never deleted."
```

---

### Task 2: Configure the Buf Schema Registry module

Names the BSR module so `buf push` (Task 6) and `buf breaking` (Task 5) have a target.

**Files:**
- Modify: `buf.yaml`

**Interfaces:**
- Produces: `buf.yaml` with a `name:` field, required by every later `buf push`/`buf breaking --against` invocation in this plan.

- [ ] **Step 1: Add the module name to `buf.yaml`**

```yaml
version: v2
name: buf.build/liquidmetal-dev/battery
modules:
  - path: api/proto
deps:
  - buf.build/liquidmetal-dev/flintlock
lint:
  use:
    - BASIC
    - FILE_LOWER_SNAKE_CASE
  except:
    - ENUM_NO_ALLOW_ALIAS
    - IMPORT_NO_PUBLIC
    - IMPORT_USED
    - PACKAGE_DIRECTORY_MATCH
    - PACKAGE_SAME_DIRECTORY
    - PACKAGE_SAME_CSHARP_NAMESPACE
    - PACKAGE_SAME_GO_PACKAGE
    - PACKAGE_SAME_JAVA_MULTIPLE_FILES
    - PACKAGE_SAME_JAVA_PACKAGE
    - PACKAGE_SAME_PHP_NAMESPACE
    - PACKAGE_SAME_RUBY_PACKAGE
    - PACKAGE_SAME_SWIFT_PREFIX
breaking:
  use:
    - FILE
```

(Only the `name:` line is new — everything else matches the existing file.)

- [ ] **Step 2: Verify lint still passes**

```bash
buf lint
```

Expected: no output, exit code 0.

- [ ] **Step 3: Commit**

```bash
git add buf.yaml
git commit -m "chore: name the buf schema registry module"
```

---

### Task 3: Add the `poolmgrd` container Dockerfile

A minimal distroless image that GoReleaser (Task 4) copies the prebuilt per-arch binary into — no compiler in the image build.

**Files:**
- Create: `Dockerfile`

**Interfaces:**
- Consumes: a `poolmgrd` binary present in the Docker build context under the name `poolmgrd` (GoReleaser's `dockers` config in Task 4 sets this context up per-arch).
- Produces: an image with `ENTRYPOINT ["/poolmgrd"]`, consumed by `docker_manifests` in Task 4.

- [ ] **Step 1: Write the Dockerfile**

```dockerfile
FROM gcr.io/distroless/static-debian12:nonroot
COPY poolmgrd /poolmgrd
ENTRYPOINT ["/poolmgrd"]
```

- [ ] **Step 2: Verify it builds standalone with a throwaway binary**

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/poolmgrd-test ./cmd/poolmgrd
cp /tmp/poolmgrd-test poolmgrd
docker build -t poolmgrd-test:local .
rm poolmgrd /tmp/poolmgrd-test
```

Expected: `docker build` succeeds. Clean-up removes the temporary binary from the repo root so it isn't accidentally committed.

- [ ] **Step 3: Commit**

```bash
git add Dockerfile
git commit -m "feat: add poolmgrd container image Dockerfile"
```

---

### Task 4: Add the GoReleaser configuration

Defines the binary build matrix, archives, multi-arch Docker images, and changelog grouping.

**Files:**
- Create: `.goreleaser.yaml`

**Interfaces:**
- Consumes: `./cmd/poolmgrd` (build entrypoint, confirmed present by Task 1), `Dockerfile` (Task 3).
- Produces: on `goreleaser release`, tarball archives, a `checksums.txt`, and `ghcr.io/liquidmetal-dev/poolmgrd:{{ .Version }}` + `:latest` multi-arch manifests — consumed by `release.yml` (Task 6) and dry-run by `ci.yml` (Task 5).

- [ ] **Step 1: Write `.goreleaser.yaml`**

```yaml
version: 2

project_name: poolmgrd

before:
  hooks:
    - go mod tidy

builds:
  - id: poolmgrd
    main: ./cmd/poolmgrd
    binary: poolmgrd
    env:
      - CGO_ENABLED=0
    goos:
      - linux
    goarch:
      - amd64
      - arm64

archives:
  - id: poolmgrd
    ids:
      - poolmgrd
    formats:
      - tar.gz

checksum:
  name_template: "checksums.txt"

dockers:
  - id: poolmgrd-amd64
    ids:
      - poolmgrd
    goos: linux
    goarch: amd64
    use: buildx
    dockerfile: Dockerfile
    image_templates:
      - "ghcr.io/liquidmetal-dev/poolmgrd:{{ .Version }}-amd64"
    build_flag_templates:
      - "--platform=linux/amd64"
  - id: poolmgrd-arm64
    ids:
      - poolmgrd
    goos: linux
    goarch: arm64
    use: buildx
    dockerfile: Dockerfile
    image_templates:
      - "ghcr.io/liquidmetal-dev/poolmgrd:{{ .Version }}-arm64"
    build_flag_templates:
      - "--platform=linux/arm64"

docker_manifests:
  - name_template: "ghcr.io/liquidmetal-dev/poolmgrd:{{ .Version }}"
    image_templates:
      - "ghcr.io/liquidmetal-dev/poolmgrd:{{ .Version }}-amd64"
      - "ghcr.io/liquidmetal-dev/poolmgrd:{{ .Version }}-arm64"
  - name_template: "ghcr.io/liquidmetal-dev/poolmgrd:latest"
    image_templates:
      - "ghcr.io/liquidmetal-dev/poolmgrd:{{ .Version }}-amd64"
      - "ghcr.io/liquidmetal-dev/poolmgrd:{{ .Version }}-arm64"

changelog:
  sort: asc
  filters:
    exclude:
      - "^docs:"
      - "^test:"
      - "^ci:"
      - "^chore:"
      - "Merge pull request"
  groups:
    - title: "Features"
      regexp: '^.*?feat(\([[:word:]]+\))??!?:.+$'
      order: 0
    - title: "Bug fixes"
      regexp: '^.*?fix(\([[:word:]]+\))??!?:.+$'
      order: 1
    - title: "Other"
      order: 999
```

- [ ] **Step 2: Validate the config statically**

```bash
mise install
goreleaser check
```

Expected: `goreleaser check` reports the config is valid.

- [ ] **Step 3: Run a local snapshot build (no publish)**

```bash
docker buildx create --use --name poolmgrd-builder || true
goreleaser release --snapshot --clean
docker buildx rm poolmgrd-builder || true
```

Expected: succeeds, producing `dist/poolmgrd_linux_amd64*/poolmgrd`, `dist/poolmgrd_linux_arm64*/poolmgrd`, `dist/checksums.txt`, and local (unpushed) images tagged `ghcr.io/liquidmetal-dev/poolmgrd:<snapshot-version>-amd64` / `-arm64`. Requires Docker with buildx and QEMU (`docker run --privileged --rm tonistiigi/binfmt --install all`) if cross-building arm64 on an amd64 dev machine.

- [ ] **Step 4: Commit**

```bash
git add .goreleaser.yaml
git commit -m "feat: add goreleaser config for poolmgrd binaries and image"
```

---

### Task 5: Extend CI with breaking-change detection and a GoReleaser dry-run

Catches proto breakage and release-config breakage on every PR, before a tag is ever cut.

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `buf.yaml`'s `name:` (Task 2), `.goreleaser.yaml` + `Dockerfile` (Tasks 3–4).

- [ ] **Step 1: Add a `buf breaking` step to the existing `proto` job**

Edit the `proto` job so it reads:

```yaml
  proto:
    name: Proto
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: jdx/mise-action@c2a87611a18de5b3828c5652fe268e992400cb5c # v4.3.0
      - run: buf lint
      # First run after this lands must publish an initial BSR version
      # (cut a release tag, or `buf push` manually) before this step has
      # anything to compare against.
      - run: buf breaking --against 'buf.build/liquidmetal-dev/battery'
      - run: ./hack/generate-proto.sh
      - run: git diff --exit-code -- api/proto
```

(Only the `buf breaking` line plus its preceding comment are new.)

- [ ] **Step 2: Add a `goreleaser-snapshot` job**

Append to `.github/workflows/ci.yml`:

```yaml
  goreleaser-snapshot:
    name: GoReleaser snapshot
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          fetch-depth: 0
      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
        with:
          go-version: "1.25"
      - uses: docker/setup-qemu-action@29109295f81e9208d7d86ff1c6c12d2833863392 # v3.6.0
      - uses: docker/setup-buildx-action@e468171a9de216ec08956ac3ada2f0791b6bd435 # v3.11.1
      - uses: goreleaser/goreleaser-action@f06c13b6b1a9625abc9e6e439d9c05a8f2190e94 # v7.2.3
        with:
          distribution: goreleaser
          version: "~> v2"
          args: release --snapshot --clean
```

- [ ] **Step 3: Validate the workflow YAML parses**

```bash
python3 -c "import yaml, sys; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo OK
```

Expected: `OK`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add buf breaking check and goreleaser snapshot dry-run"
```

---

### Task 6: Add the tag-triggered release workflow

Ties GoReleaser and `buf push` together, firing only on `v*.*.*` tags.

**Files:**
- Create: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: `.goreleaser.yaml` (Task 4), `buf.yaml` `name:` (Task 2). Requires a `BUF_TOKEN` repository secret (Buf Schema Registry API token with write access to `liquidmetal-dev/battery`) to exist before the first tag is pushed — provisioning it is a manual, out-of-band step for whoever owns the BSR org, not something this PR can automate.

- [ ] **Step 1: Write `.github/workflows/release.yml`**

```yaml
name: Release

on:
  push:
    tags:
      - "v*.*.*"

permissions:
  contents: write
  packages: write

jobs:
  release:
    name: Release
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          fetch-depth: 0

      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
        with:
          go-version: "1.25"

      - uses: jdx/mise-action@c2a87611a18de5b3828c5652fe268e992400cb5c # v4.3.0

      - uses: docker/setup-qemu-action@29109295f81e9208d7d86ff1c6c12d2833863392 # v3.6.0

      - uses: docker/setup-buildx-action@e468171a9de216ec08956ac3ada2f0791b6bd435 # v3.11.1

      - uses: docker/login-action@5e57cd118135c172c3672efd75eb46360885c0ef # v3.6.0
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - uses: goreleaser/goreleaser-action@f06c13b6b1a9625abc9e6e439d9c05a8f2190e94 # v7.2.3
        with:
          distribution: goreleaser
          version: "~> v2"
          args: release --clean
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      - run: buf push
        env:
          BUF_TOKEN: ${{ secrets.BUF_TOKEN }}
```

- [ ] **Step 2: Validate the workflow YAML parses**

```bash
python3 -c "import yaml, sys; yaml.safe_load(open('.github/workflows/release.yml'))" && echo OK
```

Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: add tag-triggered release workflow"
```

---

### Task 7: Update documentation for the release process

Records the one manual prerequisite (`BUF_TOKEN` secret + first BSR push) and how to cut a release, so it isn't tribal knowledge.

**Files:**
- Modify: `README.md` (add a "Releasing" section if one doesn't already exist; if `README.md` doesn't exist at the repo root, create it with just this section)

**Interfaces:**
- None — this is documentation only.

- [ ] **Step 1: Check whether `README.md` exists and has a releasing/release section**

```bash
test -f README.md && grep -n "^#" README.md || echo "no README.md"
```

- [ ] **Step 2: Add the section**

Append (or create `README.md` with) the following:

```markdown
## Releasing

Releases are cut by pushing a semver tag matching `v*.*.*` (e.g. `v0.1.0`)
to `main`. This triggers `.github/workflows/release.yml`, which:

- builds `poolmgrd` for `linux/amd64` and `linux/arm64` via GoReleaser
- publishes a multi-arch container image to
  `ghcr.io/liquidmetal-dev/poolmgrd:<tag>` (and `:latest`)
- creates a GitHub release with a changelog grouped by commit type
- pushes `api/proto` (`PoolAdmin`/`Lease`/`Events`/`types`) to the Buf
  Schema Registry at `buf.build/liquidmetal-dev/battery`

**One-time setup:** a `BUF_TOKEN` repository secret (a Buf Schema Registry
API token with write access to `liquidmetal-dev/battery`) must exist
before the first tag is pushed. Until the first successful `buf push`,
the `buf breaking` check in CI (`.github/workflows/ci.yml`) has nothing
to compare against and will fail on every PR — cut the first release (or
run `buf push` manually) as soon as this lands.

```bash
git tag v0.1.0
git push origin v0.1.0
```
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: document the release process"
```

---

## Self-Review Notes

- **Spec coverage:** every "Components" item (1–5) in the spec maps to a task — Dockerfile → Task 3, `.goreleaser.yaml` → Task 4, `release.yml` → Task 6, `buf.yaml` name → Task 2, `ci.yml` extensions → Task 5. The spec's "Prep cleanup" section maps to Task 1. Documentation of the manual `BUF_TOKEN` prerequisite (spec's "Error handling / edge cases" first bullet) is Task 7.
- **Placeholder scan:** no TBDs; every step has literal file contents or literal commands.
- **Type/name consistency:** binary name `poolmgrd`, image name `ghcr.io/liquidmetal-dev/poolmgrd`, BSR module `buf.build/liquidmetal-dev/battery`, and build id `poolmgrd` are used identically across Tasks 3, 4, 5, and 6.
