#!/usr/bin/env bash
# Explicitly bump buf dependencies (e.g. flintlock) and update buf.lock. This is a
# separate, deliberate action from hack/generate-proto.sh so that regular code generation
# stays deterministic and doesn't silently pull in newer upstream protos.
# Requires: buf (see mise.toml).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

buf dep update
