# Vector Runtime C API

通过 C ABI 将 Vector 嵌入 C/C++ 程序。独立 package `vector-runtime-c` 生成
`libvector_runtime_c.so`，不会把 Rust 的内部类型布局暴露给 C。
每个 runtime 自带 Tokio 线程池，默认两个工作线程。

## 编译和使用

在仓库根目录，使用仓库指定的 Rust 工具链及 Vector 原生构建依赖：

```sh
bash scripts/build-c-api.sh
```

产物位于 `target/c-api/`：`lib/libvector_runtime_c.so`、
`include/vector_runtime.h`、`examples/console.c` 和本说明。
默认保留 console、ClickHouse、Kafka、Vector、Prometheus，source 仅 external。
可以选择较小的功能集合，例如：

```sh
bash scripts/build-c-api.sh --no-default-features --features console,clickhouse
```

编译、运行 C 示例（在仓库根目录）：

```sh
cc -std=c11 -Wall -Wextra \
  -I target/c-api/include target/c-api/examples/console.c \
  -L target/c-api/lib -lvector_runtime_c \
  -Wl,-rpath,'$ORIGIN/lib' -o target/c-api/console
./target/c-api/console
```

示例启动 external → remap → console，分别提交一个结构化事件和 JSON 事件，
输出包含新增 `event_type` 字段的两行 JSON，然后 flush、stop、释放句柄。
头文件支持 `extern "C"`，C++ 调用同一个动态库即可，不需要 Rust 编译器。
交付 `.so` 时仍需匹配目标 OS、CPU 和系统动态库；原生构建不是跨架构通用包。

## API 与数据表示

调用顺序：

1. `vr_runtime_start(yaml, options, &runtime)` 解析配置并启动；options 可传 NULL。
2. `vr_runtime_source(runtime, name, &source)` 获取可重复使用的 source 句柄。
3. `vr_source_emit_json`，或 `vr_source_emit` / `vr_source_emit_batch` 提交事件。
4. `vr_runtime_flush` 等待已提交事件完成 Vector 的 finalization。
5. `vr_runtime_stop` 停止并排空管线，随后分别 destroy source 和 runtime。

`vr_slice` 为指针加字节长度，不要求 NUL 结尾。YAML、source 名称、字段名和
字符串必须是 UTF-8。字符串可以含 NUL；任意二进制数据使用 `VR_BYTES`。

结构化事件用 `vr_object` 表示字段数组，每个字段包含名称和带类型标签的
`vr_value`。支持 null、bool、i64、u64、f64、字符串、bytes、数组和嵌套对象。
u64 必须不超过 `INT64_MAX`，float 必须有限，bool 为 0 或 1。
字段名中的点是普通字符；对象不能有重复 key；嵌套上限 64 层。
结构化路径直接构建原生 `LogEvent`，不经过 JSON。

所有输入由调用方持有。函数返回前数据已复制，库不保留 C 内存指针。
调用期间，整个输入树必须有效、对齐、可读且不可被其他线程修改。
批量上限为 `VR_MAX_BATCH_EVENTS`（1000），整批转换成功后才入队。
`VR_TRY` 在队列满时返回 `VR_QUEUE_FULL`，可以直接用原始输入重试。
该模式只是不等待队列容量，转换和复制仍会执行。JSON 便利接口使用阻塞入队。

## 错误、线程与生命周期

所有返回 `vr_result` 的函数用 `VR_OK` 表示成功，其他值为头文件定义的错误码。
立即调用 `vr_last_error()` 获取当前线程的 UTF-8 诊断文本；不要释放该指针。
下一个返回 `vr_result` 的调用会替换它，成功会清空它；清理前先保存错误文本。
`vr_abi_version()` 可用于检查头文件与库是否使用同一 ABI 主版本。

同一个 source 可以供多个 OS 线程并发提交，runtime 操作受锁保护。
停止后已有 source 不会让 runtime 继续运行，非空提交返回 `VR_CLOSED`。
成功 stop 可以重复调用；停止失败后不能重启该 runtime。
destroy 必须与使用同一句柄的所有调用互斥，每个非空句柄只释放一次。
不要从信号处理函数或已进入 Tokio 的 Rust 异步上下文调用阻塞 API。

flush 不是远端持久化承诺，不强制关闭聚合窗口，也不等待 Prometheus 抓取。
destroy 未停止的 runtime 会请求后台清理；需要可靠排空时务必先 stop。
库首次使用后应保持加载至进程退出，不支持 `dlclose` 动态卸载或携带活动 runtime
跨 fork。Vector 的全局 schema/metrics 等约束与 Rust API 相同。

Rust unwind panic 会在 ABI 边界转换为 `VR_PANIC`，但调用方的悬空指针、错误长度、
重复释放和进程级 abort 不属于可以捕获的错误。以头文件中的完整契约为准。

## 验证

```sh
bash scripts/test-c-api.sh --offline
```

该命令运行 Rust panic 边界和线程安全检查，编译真实 C/C++ 调用程序，验证嵌套
结构化值、整数边界、无效输入、批量转换原子性、线程局部错误、四线程提交以及
停止后的句柄行为，并检查 console 实际交付的 JSON，而不只是验证返回码。
