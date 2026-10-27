#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
cargo test -p vector-runtime-c --release --locked --lib "$@"
cargo build -p vector-runtime-c --release --locked "$@"
build_dir="${CARGO_TARGET_DIR:-$repo_root/target}/release"
build_dir="$(cd "$build_dir" && pwd)"
mkdir -p "$build_dir/c-api-tests"
for name in tests/abi examples/console; do
    cc -std=c11 -Wall -Wextra -Werror -pthread \
        -I lib/vector-runtime-c/include "lib/vector-runtime-c/$name.c" \
        -L "$build_dir" -Wl,-rpath,"$build_dir" -lvector_runtime_c \
        -o "$build_dir/c-api-tests/$(basename "$name")"
done
# Check that the public header is also usable by C++ callers.
printf '#include "vector_runtime.h"\nint main() { return vr_abi_version() != VR_ABI_VERSION; }\n' |
    c++ -std=c++11 -Wall -Wextra -Werror -x c++ - \
    -I lib/vector-runtime-c/include -L "$build_dir" \
    -Wl,-rpath,"$build_dir" -lvector_runtime_c -o "$build_dir/c-api-tests/cpp"
"$build_dir/c-api-tests/cpp"
"$build_dir/c-api-tests/abi" > "$build_dir/c-api-tests/abi.jsonl"
"$build_dir/c-api-tests/console" > "$build_dir/c-api-tests/console.jsonl"
python3 - "$build_dir/c-api-tests" <<'PY'
import json
import pathlib
import sys
directory = pathlib.Path(sys.argv[1])
events = [json.loads(line) for line in (directory / 'abi.jsonl').read_text().splitlines()]
assert len(events) == 44, len(events)
assert all(event['checked'] is True for event in events)
native = next(event for event in events if event['id'] == 'native')
assert native['values'] == [-42, 1.25, 'hello', True, None]
assert native['nested'] == {'literal.key': 'value'}
assert native['min'] == -(2**63) and native['max'] == 2**63 - 1
assert native['blob'] == 'raw'
assert sum(event['id'] == 'batch1' for event in events) == 1
assert sum(event['id'] == 'batch2' for event in events) == 1
assert {(e['producer'], e['seq']) for e in events if e['id'] == 'thread'} == {
    (producer, seq) for producer in range(4) for seq in range(10)
}
example = [json.loads(line) for line in (directory / 'console.jsonl').read_text().splitlines()]
assert len(example) == 2
assert {event['pid'] for event in example} == {1234, 5678}
assert all(event['event_type'] == 'musa_malloc' for event in example)
print('C/C++ ABI and console integration checks passed (46 delivered events).')
PY
