#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
cargo build -p vector-runtime-c --release --locked "$@"
# Keep the standalone Go module's ABI header in sync with the canonical header.
cmp lib/vector-runtime-c/include/vector_runtime.h lib/vector-runtime-go/vector_runtime.h
build_dir="$(cd "${CARGO_TARGET_DIR:-$repo_root/target}/release" && pwd)"
export CGO_ENABLED=1
export CGO_LDFLAGS="${CGO_LDFLAGS:-} -L$build_dir -Wl,-rpath,$build_dir"
cd lib/vector-runtime-go
go test -race -count=1 ./...
go vet ./...
