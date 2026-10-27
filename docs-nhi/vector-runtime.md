# Vector Runtime：Rust 嵌入式 API

## 实现范围

`lib/vector-runtime` 是供宿主程序依赖的 Rust API crate，复用根项目现有的
`vector` 库。根库和 CLI 保留原来的名称；不把根库重命名为 `vector_runtime`。

已实现 Rust `rlib`、Rust API，以及 `lib/vector-runtime-python` 中的 PyO3 Python
接口和 wheel 构建。Python 扩展为 `cdylib`，内部使用自有 Tokio runtime。

C API 位于 `lib/vector-runtime-c`，生成 `libvector_runtime_c.so` 并提供
`include/vector_runtime.h`。执行 `bash scripts/build-c-api.sh` 可将动态库、头文件
和 C 示例整理至 `target/c-api/`。接口使用不透明句柄，支持 JSON 和直接构建
`LogEvent` 的结构化值/批量提交，内部持有 Tokio runtime。完整用法与生命周期
契约见 [C API 说明](../lib/vector-runtime-c/README.md)。
面向通用 C 调用方的 C ABI / staticlib 导出函数仍留到后续。

数据路径：

```text
宿主结构体 / LogEvent / Value / JSON
                  ↓ Rust API：转换、提交与 finalizer
         external source（原生 LogEvent）
                  ↓ SourceSender（直接使用已有有界队列）
        remap / filter / route / aggregate / reduce / log_to_metric
                  ↓
       Console / ClickHouse / Kafka / Vector / Prometheus
```

external source 不依赖结构体宏，不增加中间 mpsc 队列，不通过 JSON 转换原生事件。
每个 runtime 拥有独立的 `ExternalSourceRegistry`，通过 `ExtraContext` 传入 topology。

## 编译与 feature 裁剪

默认构建只选择 API crate，不使用 `--workspace`：

```sh
cargo build -p vector-runtime --lib --release
```

Rust 库产物为 `target/release/libvector_runtime.rlib`。应用应通过 Cargo path/git
依赖来链接库，以便 Cargo 管理传递依赖，不建议手工拷贝单个 rlib。

```toml
[dependencies]
vector-runtime = { path = "/path/to/vector/lib/vector-runtime" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "time"] }
```

API crate 对根库设置 `default-features = false`：

| 组件 | 默认启用 |
| --- | --- |
| Source | 仅 `external` |
| Transform | `remap`, `filter`, `route`, `aggregate`, `reduce`, `log_to_metric` |
| Sink | `console`, `clickhouse`, `kafka`, `vector`, `prometheus_exporter`, `prometheus_remote_write` |
| Daemon 可选功能 | 不启用 GraphQL API、API client、secrets、enrichment tables、jemalloc 等根默认 features |

五类 sink 分别由 facade 的 `console`、`clickhouse`、`kafka`、`vector`、`prometheus` features
控制，默认全部启用。只要 ClickHouse 时：

```toml
vector-runtime = { path = "/path/to/vector/lib/vector-runtime", default-features = false, features = ["clickhouse"] }
```

```sh
cargo build -p vector-runtime --lib --release --no-default-features --features clickhouse
```

这裁剪的是组件及可选依赖；根库已有的公共依赖和 VRL 标准库仍会编译。
Kafka 仍需要 librdkafka 相关原生构建依赖，Vector sink 需要 protobuf 构建工具。
Cargo features 会取并集：同一次构建如果其他包启用了根 `vector` 的默认 features，
依赖图会再次扩大。因此嵌入式构建不要同时选择根 CLI 或 `--workspace`。
API 解析配置时还会检查 source/sink 白名单，即使 feature 被其他依赖合并也不会
允许在嵌入式配置中使用其他 source/sink。

## 自有 Tokio runtime（同步宿主）

`owned-runtime` 是可选 feature，默认不开启。它控制是否提供创建、持有 Tokio
runtime 的 API；不开启时仍然依赖 Tokio 来运行异步管线，并非移除 Tokio 依赖。
开启后异步 API 仍然保留，不会因 Cargo feature 合并改变 `start()` 的行为。

```toml
[dependencies]
vector-runtime = { path = "/path/to/vector/lib/vector-runtime", default-features = false, features = ["clickhouse", "owned-runtime"] }
```

宿主无需直接依赖 Tokio，也无需 `#[tokio::main]`：

```rust,no_run
use vector_runtime::{ExternalSourceEvent, Pipeline};

#[derive(ExternalSourceEvent)]
struct MusaMallocEvent { pid: u32, size: u64 }

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let yaml = std::fs::read_to_string("pipeline.yaml")?;
    let runtime = Pipeline::from_yaml(&yaml)?.start_owned()?;
    let mut source = runtime.source("musa_malloc")?;
    source.emit(MusaMallocEvent { pid: 42, size: 4096 })?;
    runtime.flush()?;
    runtime.stop()?;
    Ok(())
}
```

`start_owned()` 返回 `OwnedRuntime`，默认创建 2 个 Tokio worker threads，启用 I/O
和 timer。可用 `start_owned_with_workers(n)` 显式指定正整数线程数。
`OwnedRuntime` 和 `BlockingSourceHandle` 提供与异步版本对应的同步发送、flush、
stop 方法，不加 `.await`。source 句柄可克隆后分发给不同 OS 线程；它不会延长管线寿命。

阻塞方法应从普通同步线程调用。在已经进入 Tokio 的上下文中调用会返回
`Error::BlockingInAsyncContext`，避免嵌套 block_on 导致 panic；此时应使用异步模式。
非阻塞 `try_emit*` 不受这一限制。

显式 `stop()` 等待排空后关闭私有 executor。Drop 立即关闭入口，并把排空和 runtime
清理移交后台线程，不阻塞调用方；因此从异步上下文 drop 也不会触发 Tokio 的
runtime-drop panic。进程退出前仍应显式 stop，以等待完成并观察错误。

## 从 fork 的 Git 分支依赖

`vector-runtime` 是可被外部项目独立依赖的 crate，但实现仍依赖同仓库的根库和
其他 workspace crates，不是把 `lib/vector-runtime` 单个目录复制出去就能构建。
Cargo 的 Git 依赖会查找仓库里的对应 package，并解析同仓库的 path/workspace 依赖。
不需要把每个内部 crate 单独发布到 crates.io，`publish = false` 也不妨碍 Git 依赖。

当前仓库分支的使用方式（需要先提交并推送本地实现）：

```toml
[dependencies.vector-runtime]
git = "https://github.com/zjnc-tech/vector.git"
branch = "feature/1474-vector-runtime"
default-features = false
features = ["clickhouse", "owned-runtime"]
```

URL 指向仓库根目录，不是 `lib/vector-runtime` 子目录；不要同时配置 `git` 与 `path`。
需要默认五类 sink 时保留默认 features，并额外启用 `owned-runtime`。
需要异步模式时去掉 `owned-runtime`，使用 `start().await`。
私有仓库需要给构建环境配置相应 Git 访问凭据。

**根 workspace 配置不会全部继承。** 本 fork 根目录的 `[patch.crates-io]`
只对当前构建的根项目生效。为了使用与 fork 相同的修复，外部宿主的根 Cargo.toml
也应加入以下内容（不是放进其某个子 crate）：

```toml
[patch.crates-io]
tokio-util = { git = "https://github.com/vectordotdev/tokio", branch = "tokio-util-0.7.13-framed-read-continue-on-error" }
nix = { git = "https://github.com/vectordotdev/nix.git", branch = "memfd/gnu/musl" }
ntapi = { git = "https://github.com/MSxDOS/ntapi.git", rev = "24fc1e47677fc9f6e38e5f154e6011dc9b270da6" }
```

宿主也不会自动使用本仓库的 `Cargo.lock`、`rust-toolchain.toml` 和 `.cargo/config.toml`。
VRL 已在 workspace.dependencies 中固定为兼容的 commit
`d802b308d7b9d96f8dc5cd2d8e2f726bb9ca345d`，避免外部宿主解析到不兼容的 main。
serde-toml-merge 也固定为 `=0.3.8`，因为 0.3.11 的公开 API 改用了 TOML 0.9，
与本项目的 TOML 0.8 类型不兼容。仅写 `version = "0.3.8"` 仍允许升级到 0.3.11。
其他未固定的传递依赖仍可能与本仓库已验证版本不同。新宿主若要从已验证依赖集合开始，可先复制本仓库的 Cargo.lock，
让 Cargo 为宿主更新，再提交宿主自己的 lockfile；已有宿主则应检查合并后的版本。
本次独立宿主检查采用了上述 patches 和已有 lockfile，验证重命名依赖 `flow`、
派生宏与无宿主 Tokio 依赖的同步 API；未验证全新锁文件的任意最新传递依赖。

Cargo.lock 会记录实际 Git commit。需要主动更新分支时执行：

```sh
cargo update -p vector-runtime
```

需要固定 fork 版本时用 `rev = "完整 commit SHA"` 替换 `branch`，并保留宿主的 lockfile。
Git 依赖语法见 [Cargo 官方文档](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-dependencies-from-git-repositories)。

## Rust 使用示例

```rust,no_run
use vector_runtime::{ExternalSourceEvent, Pipeline};

#[derive(ExternalSourceEvent)]
struct MusaMallocEvent {
    pid: u32,
    size: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::from_yaml(r#"
sources:
  musa_malloc:
    type: external
transforms:
  normalize:
    type: remap
    inputs: [musa_malloc]
    source: '.event_type = "musa_malloc"'
sinks:
  clickhouse:
    type: clickhouse
    inputs: [normalize]
    endpoint: http://localhost:8123
    database: default
    table: musa_malloc
    compression: none
    batch:
      max_events: 1000
      timeout_secs: 1
"#)?;

    let runtime = pipeline.start().await?;
    for warning in runtime.warnings() {
        eprintln!("configuration warning: {warning}");
    }
    runtime.emit("musa_malloc", MusaMallocEvent { pid: 42, size: 4096 }).await?;

    // 每个生产者缓存自己的句柄；省去每次发送的 registry 查找。
    let mut source = runtime.source("musa_malloc")?;
    source.emit(MusaMallocEvent { pid: 42, size: 8192 }).await?;

    runtime.flush().await?;
    runtime.stop().await?;
    Ok(())
}
```

默认的异步模式由宿主负责 Tokio runtime（启用 I/O 和 timer），API 不创建第二个
runtime。启用 `owned-runtime` 后也可以选择下述同步模式，由库管理私有 runtime。
两种模式均由宿主选择是否安装 tracing subscriber，库不安装 OS 信号处理器。
`from_yaml` 解析单个 YAML 文档；图校验、VRL 编译、组件构建和 healthcheck
在 `start` 中完成。环境变量插值、配置文件合并由宿主处理。
不支持 provider、secret、enrichment_tables、GraphQL api 配置。

## API 与数据转换

`Pipeline`：

- `from_yaml(&str) -> Result<Pipeline, Error>`。
- `with_shutdown_timeout(Duration)`：关闭排空期限，默认 60 秒。
- `with_flush_timeout(Duration)`：单次 flush 期限，默认 30 秒。
- `start().await -> Result<RuntimeHandle, Error>`：消费配置并启动管线。

`RuntimeHandle`：

- `source(name)`：获取可克隆的 `SourceHandle`，每个生产者使用独立句柄。
- `emit(name, event).await`：接受 `TryInto<Event>`，最终必须为 log。
- `emit_batch(name, Vec<LogEvent>).await`：原生批量提交，保留 Vec 存储。
- `try_emit(name, LogEvent)`、`try_emit_batch(name, Vec<LogEvent>)`：非阻塞提交。
- `emit_value(name, Value).await`：根节点必须为 object。
- `emit_json(name, &str).await`：解析 JSON object，拒绝超出 i64 范围的整数。
- `emit_serializable(name, &T).await`：Serde 便利路径，经中间 JSON value；
  遵循 serde_json 的语义（例如非有限浮点数序列化为 null）。热路径推荐 derive。
- `flush().await`、`status()`、`warnings()`、`stop().await`。

`SourceHandle` 提供同样的发送方法，但省略 name 参数，以 `&mut self` 调用。
句柄内部直接持有 `SourceSender`；不在异步发送期间持有共享 mutex。

API 重新导出 `ExternalSourceEvent`、`Event`、`LogEvent`、`Value` 和 `event` 模块。
结构体 derive 自动识别 `vector-runtime` 依赖（包括 Cargo 中重命名的依赖），
不要求宿主直接依赖 `vector-core` 或 `vector-lib`。生成的是可失败的
`TryFrom<T> for Event`，不是不能报告溢出错误的 `From<T>`。
支持类型和字段重命名见 [结构体转换设计](vector-runtime-struct.md)。

原生 `LogEvent` 不发生序列化；derive 直接生成原生字段并检查整数溢出、
NaN/Infinity。JSON/Serde 适合作为兼容入口，不作为最佳性能路径。

## 背压、所有权与完成语义

- `emit` 成功只表示进入 source 有界队列，不代表 sink 已交付。
- 队列满时异步发送等待容量；关闭会唤醒等待者并返回错误。
- `try_emit*` 的 `Full`、`Closed`、`TooLarge`、`UnknownSource` 错误保留原生输入，
  可通过 `into_inner()` 取回。一个批次要么全部入队，要么全部返回。
- 批次上限为导出的 `MAX_BATCH_EVENTS`（当前 1000）。更大的数据由调用方分批。
- 失败重试不会残留 API 添加的 finalizer，但会保留调用方原有 finalizer。
  返回事件可能已附加 source metadata。
- 取消异步发送 future 可能丢弃尚未入队的输入；需要重试所有权时使用 try API。

`flush` 为每次非空提交附加一个共享 `BatchNotifier`，每个事件持有其 finalizer。
最后一个引用释放时，通过同步短回调更新 tracker，不为每条消息创建 Tokio task。
调用 flush 时记录提交序号边界，只等待此前登记的提交；之后的并发提交不延长
本次等待。调用者应先 await 自己的发送任务，再调用 flush。

完成状态遵循 Vector finalization：过滤掉的事件也算完成；fan-out 等待各分支
释放 finalizer。任意批次出现 Errored/Rejected，flush 返回 `Error::Delivery`，
后续 flush 仍报告该错误（失败状态在本次 runtime 生命周期内保留）。

flush 不主动触发 aggregate/reduce 的时间窗口，不保证数据库磁盘持久化，
也不等待 Prometheus exporter 被抓取。长窗口可能导致 flush 超时；可增大期限，
或通过 stop 关闭输入让支持排空的组件完成处理。远端 Vector 的确认语义还取决于
远端 source/sink 的 acknowledgement 配置。

## 生命周期与进程级限制

`status` 为 Running / Stopping / Stopped / Failed。组件任务异常由 supervisor
观察并关闭管线。`stop` 消费运行时句柄，先停止接收新数据，再排空组件并等待完成；
超过期限则 abort topology 任务并返回超时错误，未交付事件可能丢失。
保留的 `SourceHandle` 在 stop/drop 后不能继续发送。

Drop 会立即关闭入口并请求后台排空；它不能异步等待。需要可靠地等待结束时，
在退出宿主 Tokio runtime 之前调用 `stop().await`。取消 stop future 不取消
已经发出的关闭请求。abort 无法中断正在执行的同步计算，也不保证组件内部
自行启动的后台任务立即退出。

多个 runtime 的 topology 和 external registry 相互独立，但 Vector 的 metrics、
log schema、telemetry 是进程级全局状态。当前 API 只允许默认 log schema、telemetry
和 metrics expiry 设置，避免多个管线互相修改这些全局配置。
已有 Vector metrics controller 会被复用；若宿主已经安装不兼容的 metrics recorder，
start 会返回明确错误。不同管线使用相同组件名时，内部 metrics 标签可能合并；
需要区分时应由宿主使用不同组件名。

## 验证

```sh
cargo check -p vector-runtime
cargo test -p vector-runtime --no-default-features --features clickhouse,owned-runtime
cargo test -p vector-runtime --no-default-features --features clickhouse
cargo test -p vector-external-macros
```

集成测试以本地 HTTP 服务模拟 ClickHouse，覆盖 typed/native/JSON/Value 输入、
批量提交、flush 等待响应、拒收、关闭超时、保留句柄、多个 runtime 隔离、
配置错误与原有 finalizer 的所有权。Kafka、Vector、Prometheus 的编译组合会检查；
这些远端系统的真实交付需在部署环境继续做集成验证。

## Python API 与 wheel

Python 使用独立的 `vector-runtime-python` Cargo package，发行包名为 `vector-runtime`，
导入名为 `vector_runtime`。默认仍只包含 external source 和 console、ClickHouse、Kafka、Vector、Prometheus sinks。

```sh
maturin build --release --locked --manifest-path lib/vector-runtime-python/Cargo.toml --out target/wheels
python3 -m pip install target/wheels/vector_runtime-*.whl
```

```python
from vector_runtime import Pipeline, external_event

@external_event
class MusaMallocEvent:
    pid: int
    size: int

with Pipeline.from_yaml(open("pipeline.yaml").read()).start() as runtime:
    source = runtime.source("musa_malloc")
    source.emit({"pid": 42, "size": 4096})
    source.emit(MusaMallocEvent(pid=42, size=8192))
    runtime.flush()
```

`dict` 直接转换成原生 LogEvent，不经过 JSON。`@external_event` 是可选的 dataclass
装饰器，记录字段布局供 Rust 直接读属性；不调用 asdict，不做递归深复制。
Python 不调用 Rust 的编译期 derive 宏。转换仍会把 Python 数据复制到 Rust 所有的
Value 中，并检查 i64 溢出、非有限浮点数、字典 key 类型和递归深度。

完整 API、错误类型、线程和关闭语义见
[Python 包说明](../lib/vector-runtime-python/README.md)。
当前构建产物为 CPython 3.8+ ABI3、Linux x86_64、manylinux_2_31 wheel；
本机使用 CPython 3.8 验证，其他 Python 版本未逐一运行。构建产物在 target/wheels，
不自动上传到 PyPI。

## Console 调试示例

仓库根目录的 `pipeline.yaml` 演示 external → remap → console。
source 名为 `musa_malloc`，remap 添加 `event_type: "musa_malloc"`，console 向 stdout
输出 JSON。Rust/Python 包均默认启用 console；使用 default-features=false 时需显式
加入 `console` feature。旧版 wheel 需要更新到重新构建的包。

```sh
cargo run -p vector-runtime --example console --features owned-runtime
```
