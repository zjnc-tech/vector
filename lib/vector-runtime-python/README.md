# vector-runtime Python

PyO3 bindings for this fork's embedded Vector runtime. Python calls use a private
Tokio runtime; the host does not install or configure Tokio. The default build
contains only the external source and Console, ClickHouse, Kafka, Vector and Prometheus
sinks, with the Rust runtime's common transforms.

```python
from vector_runtime import Pipeline, external_event
from dataclasses import field

@external_event
class MallocEvent:
    pid: int = field(metadata={"external_name": "process_id"})
    size: int = 0

with Pipeline.from_yaml(open("pipeline.yaml").read()).start() as runtime:
    source = runtime.source("musa_malloc")  # Matches the repository pipeline.yaml demo
    source.emit({"process_id": 42, "size": 4096})
    source.emit(MallocEvent(pid=42, size=8192))
    source.emit_batch([{"process_id": 42, "size": 16}, MallocEvent(42, 32)])
    runtime.flush()
```

`dict` is the primary interface: Rust converts it directly into `LogEvent`, with
no JSON serialization. The optional `@external_event` creates/accepts a dataclass
and records its field layout; Rust reads those attributes directly without an
intermediate dict or `dataclasses.asdict()` deep copy. It also works above an
existing `@dataclass(frozen=True)` decorator. Annotations are documentation;
conversion validates the actual values. Nested dataclass fields are not supported;
use nested dictionaries. Rust's derive macro remains a Rust compile-time facility.

Values: `None`, `bool`, signed 64-bit `int`, finite `float`, `str`, `bytes`,
`list`, `tuple`, and `dict` with string keys. Nesting is limited to 64 levels;
cyclic containers, unsupported types, integer overflow and NaN/Infinity are
rejected. Field names containing dots remain literal keys. Python strings and
containers are copied into owned Rust values before releasing the GIL; this is
not zero-copy. Unsupported values raise TypeError, overflow raises OverflowError,
and invalid values/configuration raise ValueError.

## API

- `Pipeline.from_yaml(yaml, *, worker_threads=2, shutdown_timeout=60.0, flush_timeout=30.0)`
  or `Pipeline(yaml, ...)`; `start()` consumes the parsed pipeline once, returning Runtime.
- `Runtime.source(name)` returns a reusable, thread-safe SourceHandle.
- Runtime: `emit(name, event)`, `emit_batch(name, events)`, `emit_json(name, json)`,
  `try_emit(name, event)`, `try_emit_batch(name, events)`.
- SourceHandle: the same send methods without the source name.
- Runtime: `flush()`, `stop()` / `close()`, `status` and `warnings` properties.
  A stopped runtime cannot restart. Successful stop/close is idempotent.
- Both runtime and source methods can be called by Python threads. Blocking Rust
  work releases the GIL. Lifecycle operations are serialized; a concurrent stop
  can wait for an active flush (up to its configured deadline). Sends release the
  lifecycle lock before waiting for queue capacity, so stop can cancel them.
- `try_emit*` returns None on success, raises QueueFullError on backpressure.
  Python input remains available and is not mutated; retry that same input.
- Batches are list/tuple, at most MAX_BATCH_EVENTS (1000). Conversion completes
  before any event is submitted, so invalid batches are not partially enqueued.
- ClosedError: stopped source/runtime. DeliveryError: rejected/errored submission.
  TimeoutError: flush/shutdown deadline. KeyError: unknown source.
  Other lifecycle/build failures raise VectorRuntimeError.

`emit` means queued, `flush` waits for Vector finalization. It does not force
aggregate windows, guarantee remote disk durability, or wait for a Prometheus
scrape. Delivery errors are sticky for the lifetime of a runtime.
Use `with` or explicit stop before process exit. Dropping a runtime requests
background draining, which cannot be guaranteed during interpreter/process exit.
Source handles do not keep the runtime alive. If a with block already raised an
exception, shutdown does not mask that original exception.

These are synchronous APIs, not asyncio awaitables. In an asyncio application,
use a thread executor for blocking calls. Do not share a running pipeline across
fork: create it inside each child process (prefer multiprocessing spawn).
Vector's process-wide metrics/schema restrictions from the Rust API still apply.

## Build and install

Run from this repository root (requires its Rust toolchain and native build deps):

```sh
maturin build --release --locked --manifest-path lib/vector-runtime-python/Cargo.toml --out target/wheels
python3 -m pip install target/wheels/vector_runtime-*.whl
```

Only ClickHouse:

```sh
maturin build --release --locked --manifest-path lib/vector-runtime-python/Cargo.toml --no-default-features --features clickhouse --out target/wheels
```

### Linux ARM64 (aarch64)

Build on an **ARM64 Linux machine or ARM64 CI runner**, from the repository root:

```sh
# Debian/Ubuntu build dependencies (install Rust/rustup separately).
sudo apt-get update
sudo apt-get install -y build-essential cmake perl pkg-config protobuf-compiler python3-dev python3-pip
python3 -m pip install 'maturin>=1.9,<2' patchelf
bash scripts/build-python-wheel.sh aarch64
```

Use the repository's pinned Rust toolchain. If your distribution's CMake or
protobuf compiler is too old for the dependencies, install a newer version.
The default console, ClickHouse, Kafka, Vector and Prometheus sinks are retained.
The script also accepts `x86_64` or `native`, and forwards additional maturin
options, for example `aarch64 --no-default-features --features clickhouse`.

ARM64 wheels are written to `target/wheels/aarch64/`; x86_64 wheels go to
`target/wheels/x86_64/`. On the ARM64 destination machine:

```sh
python3 -m pip install target/wheels/aarch64/vector_runtime-*.whl
python3 -m unittest discover -s lib/vector-runtime-python/tests -v
```

The filename ends in `_aarch64.whl`, with `cp38-abi3` for CPython 3.8+.
The actual manylinux/glibc requirement depends on the build environment and
maturin's audit. To support older Linux distributions, use an ARM64 manylinux
builder with the desired glibc baseline and keep the wheel audit enabled.
This is a Linux glibc wheel, not a macOS Apple Silicon or Alpine/musl wheel.

### Cross-compile ARM64 with Docker on x86_64 Linux

The Docker entry point reuses `scripts/cross/aarch64-unknown-linux-gnu.dockerfile`
and its ARM64 C/C++ toolchain, LLVM and protoc. It runs x86_64 build tools to
produce ARM64 code; compilation requires neither QEMU nor a privileged container.
Kafka uses its CMake build so native dependencies use the cross toolchain.

Install Docker and rustup, then run from the repository root:

```sh
python3 -m pip install 'maturin==1.9.6'
rustup target add aarch64-unknown-linux-gnu
bash scripts/build-python-wheel-docker.sh
```

The script builds the Docker image, mounts the active repository Rust toolchain
and maturin executable read-only, and mounts the repository and Cargo cache
writable. It runs as your UID/GID to avoid root-owned build artifacts. The host
maturin executable must run on the image's older Linux environment (the PyPI
maturin 1.9.6 binary works); a host-built binary may require a newer glibc.
The `abi3-py38` extension does not need an ARM64 Python interpreter to compile.

Build intermediates go to `target/python-docker/`. Wheels go to
`target/wheels/aarch64/`, with compatibility checked against `manylinux_2_24`
(glibc >= 2.24). The audit fails rather than silently allowing unbundled native
libraries. It does not perform automatic shared-library repair.

Options and cached/offline builds:

```sh
# Limit concurrency (default: 8), or select a subset of sinks.
CARGO_BUILD_JOBS=4 bash scripts/build-python-wheel-docker.sh \
  --no-default-features --features console,clickhouse

# Reuse an already-built image and cached Cargo dependencies.
WHEEL_CROSS_IMAGE=vector-cross-env:aarch64-unknown-linux-gnu \
  bash scripts/build-python-wheel-docker.sh --offline
```

`--offline` applies to Cargo. Initial Docker image creation still needs network
access unless `WHEEL_CROSS_IMAGE` names an existing local image. Keep the audit
enabled; changing `WHEEL_COMPATIBILITY` requests a different checked baseline,
not a way to relabel incompatible binaries. Run the Python integration tests
above on an ARM64 machine after installing the resulting wheel.

Wheel package name: `vector-runtime`; import: `vector_runtime`.
The extension uses CPython's stable ABI (`abi3`, Python >= 3.8), but a wheel is
still specific to its OS, CPU architecture, libc and native shared libraries.
A local `--compatibility linux --auditwheel skip` build needs compatible system
libraries; do not relabel it as manylinux. Portable Linux wheels should be built
and repaired in an appropriate manylinux image (repair needs patchelf).
No wheel is published automatically. PyO3 and maturin do not change Cargo's
feature-unification rules; build this package without selecting the root CLI.

Test the installed wheel, not the source directory:

```sh
python3 -m unittest discover -s lib/vector-runtime-python/tests -v
```
