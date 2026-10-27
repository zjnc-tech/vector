#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == --help ]]; then
    cat <<'EOF'
Usage: bash scripts/build-python-wheel-docker.sh [maturin build options...]

Build a Linux ARM64 Python wheel in Vector's existing cross Docker image.
Run on x86_64 Linux with Docker, rustup and maturin 1.9.x installed.
First install the target: rustup target add aarch64-unknown-linux-gnu

Environment:
  WHEEL_CROSS_IMAGE     Existing image override; otherwise build Vector's image.
  WHEEL_COMPATIBILITY   Wheel compatibility policy (default: manylinux_2_24).
  CARGO_BUILD_JOBS     Optional build parallelism limit.

The repository and Cargo cache are mounted writable, the Rust toolchain and
maturin executable read-only. Output: target/wheels/aarch64/.
No ARM emulation or privileged container is required for compilation.
EOF
    exit 0
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
if [[ "$(uname -s)/$(uname -m)" != Linux/x86_64 ]]; then
    echo 'This cross image requires an x86_64 Linux host.' >&2
    exit 1
fi
for tool in docker rustup maturin; do
    command -v "$tool" >/dev/null || { echo "Missing tool: $tool" >&2; exit 1; }
done
target=aarch64-unknown-linux-gnu
if ! rustup target list --installed | grep -qx "$target"; then
    echo "Install the Rust target first: rustup target add $target" >&2
    exit 1
fi
rustc_path="$(rustup which rustc)"
toolchain_dir="$(dirname "$(dirname "$rustc_path")")"
maturin_path="$(command -v maturin)"
cargo_cache="${CARGO_HOME:-$HOME/.cargo}"
cargo_cache="$(cd "$cargo_cache" && pwd)"
image="${WHEEL_CROSS_IMAGE:-vector-cross-env:$target}"
if [[ -z "${WHEEL_CROSS_IMAGE:-}" ]]; then
    docker build -t "$image" -f "scripts/cross/$target.dockerfile" .
fi

mkdir -p target/wheels/aarch64
docker run --rm --user "$(id -u):$(id -g)" \
    --mount "type=bind,src=$repo_root,dst=$repo_root" \
    --mount "type=bind,src=$cargo_cache,dst=$cargo_cache" \
    --mount "type=bind,src=$toolchain_dir,dst=/wheel-rust,readonly" \
    --mount "type=bind,src=$maturin_path,dst=/wheel-tools/maturin,readonly" \
    --workdir "$repo_root" \
    --env "CARGO_HOME=$cargo_cache" \
    --env "CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-8}" \
    --env 'PATH=/wheel-tools:/wheel-rust/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' \
    --env PYO3_NO_PYTHON=1 \
    --env CARGO_TARGET_DIR=target/python-docker \
    --env JEMALLOC_SYS_WITH_LG_PAGE=16 \
    "$image" \
    maturin build --release --locked \
    --manifest-path lib/vector-runtime-python/Cargo.toml \
    --target "$target" \
    --compatibility "${WHEEL_COMPATIBILITY:-manylinux_2_24}" \
    --auditwheel check \
    --out target/wheels/aarch64 "$@"
