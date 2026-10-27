#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: bash scripts/build-python-wheel.sh [native|aarch64|x86_64] [maturin options...]

Build a Linux wheel on a machine with the matching CPU architecture.
Examples:
  bash scripts/build-python-wheel.sh aarch64
  bash scripts/build-python-wheel.sh native --no-default-features --features clickhouse

Requires the repository Rust toolchain, maturin, and native build dependencies.
Wheels are written to target/wheels/<architecture>/.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ "$(uname -s)" != Linux ]]; then
    echo 'This script builds Linux wheels; run it on Linux.' >&2
    exit 1
fi

host_arch="$(uname -m)"
[[ "$host_arch" != arm64 ]] || host_arch=aarch64
arch="${1:-native}"
if [[ $# -gt 0 ]]; then shift; fi
[[ "$arch" != native ]] || arch="$host_arch"
[[ "$arch" != arm64 ]] || arch=aarch64
case "$arch" in
    aarch64|x86_64) ;;
    *) echo "Unsupported architecture: $arch" >&2; exit 1 ;;
esac
if [[ "$arch" != "$host_arch" ]]; then
    echo "Requested $arch, but this host is $host_arch. Run on a $arch Linux builder." >&2
    echo 'Installing a Rust target alone does not cross-compile the native dependencies.' >&2
    exit 1
fi

for tool in cargo maturin cc c++ make cmake perl protoc pkg-config; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "Missing build tool: $tool" >&2
        exit 1
    fi
done

exec maturin build --release --locked \
    --manifest-path lib/vector-runtime-python/Cargo.toml \
    --target "$arch-unknown-linux-gnu" \
    --out "target/wheels/$arch" "$@"
