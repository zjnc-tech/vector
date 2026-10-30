# Vector Runtime Go API

通过 cgo 复用 `vector-runtime-c`，在 Go 进程内运行 Vector。结构化事件直接转换为
原生 Vector 值，不经过 JSON；每个 runtime 自带 Tokio 线程池。需要 Go 1.20+、
C 编译器、启用 cgo，以及与运行环境 OS/CPU/系统库匹配的 C API 动态库。

## 构建和运行

从仓库根目录执行（Linux）：

```sh
bash scripts/build-c-api.sh
export CGO_ENABLED=1
export CGO_LDFLAGS="-L$(pwd)/target/c-api/lib -Wl,-rpath,$(pwd)/target/c-api/lib"
cd lib/vector-runtime-go
go run ./examples/console
```

C 库默认包含 console、ClickHouse、Kafka、Vector 和 Prometheus sinks，source
为 external，与现有 C/Python API 一致。可向构建脚本传入
`--no-default-features --features console,clickhouse` 选择功能。

Go 模块自带 ABI v1 头文件，不需要使用者访问仓库中的 C 头文件目录。
`CGO_LDFLAGS` 指定动态库位置；部署时也必须提供此动态库及其系统依赖。
示例使用绝对 rpath 方便本地运行，部署时应改成目标机器上的库路径，或通过
`LD_LIBRARY_PATH` 配置动态库搜索路径。仅交付 `.so` 和 Go 模块即可编译调用方，
调用方不需要 Rust 工具链。

这是仓库内独立模块，尚未发布到远端 Go module 路径。在其他 Go 工程内使用：

```sh
go mod edit -require=vector-runtime-go@v0.0.0
go mod edit -replace=vector-runtime-go=/absolute/path/to/vector/lib/vector-runtime-go
```

```go
import vector "vector-runtime-go"

r, err := vector.Start(yaml, nil) // nil 使用默认配置
if err != nil {
    return err
}
defer r.Close() // 生产代码应检查 Close 返回的排空错误；见 examples/console

source, err := r.Source("input") // YAML 中 external source 的名字
if err != nil {
    return err
}
defer source.Close()

if err := source.Emit(vector.Event{"pid": 42, "size": uint64(4096)}); err != nil {
    return err
}
return r.Flush()
```

## 接口

- `Start(yaml string, options *Options) (*Runtime, error)` 解析并启动。
  `DefaultOptions()` 返回两个工作线程、60 秒停止超时、30 秒 flush 超时。
  超时使用 `time.Duration`，非负，按毫秒截断；0 表示零超时。
- `Runtime.Source(name)` 获取可复用的 `*Source`。
- Source：`Emit(Event)`、`EmitBatch([]Event)`、`TryEmit(Event)`、
  `TryEmitBatch([]Event)`、`EmitJSON(string)`。
- Runtime 也提供上述五种提交方法，第一个参数为 source 名字。
  高频提交建议缓存 Source，避免每次解析并创建句柄。
- `Runtime.Flush()` 等待已提交事件完成 finalization；`Status()` 返回
  `Running`、`Stopping`、`Stopped` 或 `Failed` 及错误。
- `Runtime.Stop()` 排空并停止，保留句柄以便查询状态；成功后可重复调用。
  停止后不能重新启动。`Runtime.Close()` 停止并释放句柄，即使停止失败也释放，
  同时返回该错误。重复 Close 成功；Close 后其他操作返回 `ErrClosed`。
- `Source.Close()` 只释放此句柄，不停止 runtime。

`Event` 是 `map[string]any` 的别名。值支持 nil、bool、Go 有符号/无符号整数
（不含 uintptr）、float32/float64、string、[]byte、数组、切片、字符串键 map，
以及这些类型的命名类型。uint 必须不超过 int64 最大值，浮点必须有限。
字符串和 key 必须为 UTF-8，可含 NUL；二进制用 []byte。点号 key 是字面 key。
嵌套最多 64 层，循环容器会因深度限制被拒绝；struct、指针等类型不支持，
请显式映射为 Event。nil interface 是 null；nil map/slice 分别为空对象/数组，
nil []byte 是空 bytes。输入会复制，在方法返回前不得并发修改。

批次上限 `MaxBatchEvents`（1000），整批转换、校验成功后才入队。
`TryEmit*` 仅不等待队列容量，仍会执行转换；满队列返回 `ErrQueueFull`，
可用原输入重试。JSON 接口要求对象，采用阻塞入队。空批次不提交事件，
不会检查原生 source 是否已停止，但 Go 句柄 Close 后仍返回 `ErrClosed`。

使用 `errors.Is(err, vector.ErrQueueFull)` 等识别错误；`errors.As` 到 `*vector.Error`
获取 `Code` 和原生诊断。错误码包括 `ErrInvalidArgument`、`ErrConfig`、
`ErrUnknownSource`、`ErrClosed`、`ErrQueueFull`、`ErrTimeout`、`ErrDelivery`、
`ErrInternal`、`ErrPanic`。读取原生线程局部错误时固定 OS 线程，避免 goroutine
迁移造成错误串线。

Runtime 和 Source 支持并发使用，复制 Go 包装对象共享同一句柄及关闭状态。
Go 层同步释放与调用，避免重复释放或使用已释放的 C 指针。Source 不保活 Runtime；
请保留 Runtime 并显式 Close。终结器只作为遗漏 Close 的兜底，不能保证退出前交付。
停止可取消等待队列的提交；Source.Close 会等待使用该 source 的提交返回，
需要取消阻塞提交时先停止 runtime。Stop 可能等待正在运行的 Flush 至其超时。

这些是同步接口，不接受 context；截止时间通过 Options 配置。
Emit 成功只表示入队；Flush 不强制关闭聚合窗口、不承诺远端磁盘持久化，
也不等待 Prometheus scrape。交付错误在 runtime 生命周期内保持。
不要携带运行中的 runtime 跨 fork，也不要卸载正在使用的共享库。
Rust API 的进程级 schema/metrics 约束仍然适用。

## 验证

```sh
bash scripts/test-go-api.sh --offline
```

构建 C 库、检查 Go 模块附带头文件与源头文件一致，运行 Go race 检测和 go vet。
测试使用真实 external → remap → console 管线检查交付的 JSON，包括嵌套值、
整数边界、二进制、含 NUL 字符串、批次原子性、并发提交、停止后提交以及重复关闭。
