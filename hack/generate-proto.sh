#!/usr/bin/env bash
# Regenerate the poolmgr gRPC/protobuf Go code from api/proto/**/*.proto, using the
# dependency versions pinned in the committed buf.lock. Deterministic: does not touch
# buf.lock. To bump buf dependencies (e.g. flintlock), run hack/update-proto-deps.sh first.
# Requires: buf, protoc-gen-go, protoc-gen-go-grpc (see mise.toml).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

buf generate
