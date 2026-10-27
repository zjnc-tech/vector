#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
if [[ "$(uname -s)" != Linux ]]; then
    echo 'This packaging script currently targets Linux .so libraries.' >&2
    exit 1
fi
# Arguments are Cargo feature/network options; use CARGO_BUILD_TARGET for an
# explicitly configured cross toolchain, and CARGO_TARGET_DIR for a custom cache.
cargo build -p vector-runtime-c --release --locked "$@"
target_root="${CARGO_TARGET_DIR:-$repo_root/target}"
library_dir="$target_root/${CARGO_BUILD_TARGET:+$CARGO_BUILD_TARGET/}release"
package_dir="$target_root/c-api${CARGO_BUILD_TARGET:+/$CARGO_BUILD_TARGET}"
mkdir -p "$package_dir/lib" "$package_dir/include" "$package_dir/examples"
cp "$library_dir/libvector_runtime_c.so" "$package_dir/lib/"
cp lib/vector-runtime-c/include/vector_runtime.h "$package_dir/include/"
cp lib/vector-runtime-c/examples/console.c "$package_dir/examples/"
cp lib/vector-runtime-c/README.md "$package_dir/"
echo "C API artifacts: $package_dir"
