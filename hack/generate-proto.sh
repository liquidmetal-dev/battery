#!/usr/bin/env bash
# Regenerate the poolmgr gRPC/protobuf Go code from api/proto/**/*.proto.
# Requires: buf, protoc-gen-go, protoc-gen-go-grpc (see mise.toml).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

buf dep update
buf generate
