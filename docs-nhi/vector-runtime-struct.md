# ExternalSourceEvent 派生宏设计

## 目标

为 [Vector Runtime](vector-runtime.md) 提供结构化 Rust 事件的编译期转换能力。宿主程序通过 derive 宏，将普通结构体直接转换为 Vector 原生 `Event::Log`，不经过 JSON 文本或 `serde_json::Value`。

```rust
#[derive(Serialize, ExternalSourceEvent)]
struct MusaMallocEvent {
    pid: u32,
    size: u64,
}

source.emit(MusaMallocEvent {
    pid: 123,
    size: 4096,
}).await?;
```

`Serialize` 与 `ExternalSourceEvent` 相互独立。外部事件转换不依赖 `Serialize`；宿主仅在其他场景需要序列化时才需要同时派生它。

本文中的 API 和代码为拟议接口及示意代码，不代表仓库已经实现。

## 转换路径

```text
用户结构体（转移所有权）
    ↓ 编译期生成的字段转换代码
ObjectMap<KeyString, Value>
    ↓ LogEvent::from(fields)
Event::Log(LogEvent)
    ↓ SourceSender
Vector topology
```

内部复用 Vector 原生 `Value`、`ObjectMap`、`LogEvent` 和 `Event`。不同结构体可以拥有不同字段，不要求所有 external source 共享一种业务结构体。

该方案省去通用序列化中间层，但不是零分配：字段名、对象容器、数组容器和 `LogEvent` 等仍可能产生分配。

## 派生 TryFrom，而不是 From

宏生成：

```rust
impl TryFrom<MusaMallocEvent> for vector_runtime::Event {
    type Error = vector_runtime::EncodeError;

    fn try_from(value: MusaMallocEvent) -> Result<Self, Self::Error> {
        // 逐字段转换并构造 Event::Log。
    }
}
```

采用 `TryFrom` 的原因：

- Vector 原生整数为 `i64`，部分 `u64` 值无法表示。
- 浮点字段需要校验非有限值。
- 数组和 map 的元素也可能转换失败。

不能使用无检查的 `as i64`，也不能自动把超范围整数转换成浮点数，否则可能截断或损失精度。转换失败返回错误，不生成部分事件，不向队列提交数据。

第一版统一生成 `TryFrom`，即使某个结构体的所有字段都可以无损转换，也不额外生成 `From`，保持接口一致并避免 trait 实现冲突。

## 支持范围

第一版支持具名字段结构体，不支持 enum、tuple struct、unit struct 和用户自定义嵌套结构体。为控制宏实现复杂度，第一版不支持带泛型参数的结构体。

### 标量类型

| Rust 类型 | Vector 原生表示 | 转换规则 |
| --- | --- | --- |
| `i8`、`i16`、`i32`、`i64` | `Value::Integer(i64)` | 无损转换 |
| `u8`、`u16`、`u32` | `Value::Integer(i64)` | 无损转换 |
| `u64`、`isize`、`usize` | `Value::Integer(i64)` | 使用检查转换，超范围报错 |
| `f32`、`f64` | `Value::Float` | 拒绝 NaN、正无穷和负无穷；有限 `f32` 提升为 `f64` |
| `String` | `Value::Bytes` | 通过 `Bytes::from(String)` 转移字符串内容所有权 |

Vector 当前浮点表示使用 `NotNan<f64>`；拒绝无穷是本 API 额外规定的编码约束。

第一版不支持 `i128`、`u128`、`bool`、`Option<T>`、借用字符串、固定长度数组及其他未列出的类型。后续可以单独扩展。

### 容器类型

设 `T` 为上表支持的标量类型：

| Rust 类型 | Vector 原生表示 | 转换规则 |
| --- | --- | --- |
| `Vec<T>` | `Value::Array(Vec<Value>)` | 按顺序逐元素转换 |
| `HashMap<String, T>` | `Value::Object(ObjectMap)` | 转换键和值，构建原生对象 |
| `BTreeMap<String, T>` | `Value::Object(ObjectMap)` | 转换键和值，构建原生对象 |

只支持一层容器，不支持 `Vec<Vec<T>>`、`Vec<Map<...>>`、map 的值为容器等嵌套形式。Map 的 key 必须是 `String`。

数组顺序保持不变。对象遵循原生 `ObjectMap` 的键排序语义，不承诺保留输入 HashMap 的迭代顺序。

数组转换需要构造 `Vec<Value>`；map 转换需要构造原生 `ObjectMap`。即使输入是 `BTreeMap<String, T>`，也不能承诺直接复用整个原容器。

## 宏展开示例

下面展示等价的直接转换代码，实际宏通过转换 trait 复用类型规则：

```rust
impl TryFrom<MusaMallocEvent> for vector_runtime::Event {
    type Error = vector_runtime::EncodeError;

    fn try_from(value: MusaMallocEvent) -> Result<Self, Self::Error> {
        let MusaMallocEvent { pid, size } = value;

        let mut fields = vector_runtime::ObjectMap::new();

        fields.insert(
            "pid".into(),
            vector_runtime::Value::Integer(i64::from(pid)),
        );

        fields.insert(
            "size".into(),
            vector_runtime::Value::Integer(
                i64::try_from(size).map_err(|_| {
                    vector_runtime::EncodeError::integer_overflow("size")
                })?,
            ),
        );

        Ok(vector_runtime::Event::Log(
            vector_runtime::LogEvent::from(fields),
        ))
    }
}
```

转换按值消费结构体，不 clone 整个事件。错误返回后，原结构体已经被消费，不承诺返回原始输入。

## 实现分层

### Runtime 类型转换层

runtime crate 提供统一的字段转换 trait：

```rust
pub trait IntoExternalValue: sealed::IntoExternalValue {
    fn into_external_value(self) -> Result<Value, ValueEncodeError>;
}
```

为每种支持的标量实现转换：

```rust
impl IntoExternalValue for String {
    fn into_external_value(self) -> Result<Value, ValueEncodeError> {
        Ok(Value::Bytes(Bytes::from(self)))
    }
}

impl IntoExternalValue for u64 {
    fn into_external_value(self) -> Result<Value, ValueEncodeError> {
        let value = i64::try_from(self)
            .map_err(|_| ValueEncodeError::IntegerOverflow)?;

        Ok(Value::Integer(value))
    }
}
```

容器实现使用仅对支持标量开放的 `ExternalScalar`：

```rust
pub trait ExternalScalar: IntoExternalValue + sealed::ExternalScalar {}

impl<T: ExternalScalar> IntoExternalValue for Vec<T> {
    // 逐元素转换，失败时补充数组下标。
}

impl<T: ExternalScalar> IntoExternalValue for HashMap<String, T> {
    // 逐项转换，失败时补充 map key。
}

impl<T: ExternalScalar> IntoExternalValue for BTreeMap<String, T> {
    // 逐项转换，失败时补充 map key。
}
```

第一版将转换 trait 的实现范围封闭：`IntoExternalValue` 同样使用 sealed supertrait，仅允许 runtime 为列出的类型实现。这样不支持的自定义类型不能通过自行实现 trait 绕过类型约束。

不能把容器实现写成递归的 `T: IntoExternalValue`，否则会自动允许嵌套容器。

### Proc-macro 层

derive 宏放在单独的 proc-macro crate 中，由 runtime crate 重导出，宿主只需要依赖 runtime。

宏负责：

1. 校验输入是受支持的具名字段结构体。
2. 读取字段名和 `external` 属性。
3. 生成按值解构、字段转换及对象构造代码。
4. 生成 `TryFrom<用户结构体> for Event`。

生成的单个字段转换类似：

```rust
fields.insert(
    "size".into(),
    vector_runtime::IntoExternalValue::into_external_value(size)
        .map_err(|error| error.at_field("size"))?,
);
```

不依赖字符串比较来判断类型，例如不通过匹配 `"Vec<String>"` 决定转换逻辑。实际类型支持交给 Rust trait 解析，因此支持类型的别名也能正常工作。

不支持的字段类型在编译期报告 trait bound 错误；宏生成代码应保留字段 span，方便定位到对应字段。

生成代码需要正确解析 runtime crate 路径，包括宿主在 Cargo.toml 中重命名依赖的情况。宿主不应为了宏展开额外直接依赖 `bytes`、`vrl` 等内部依赖。

## 字段名与属性

默认使用 Rust 字段名作为对象 key。原始标识符移除 `r#` 前缀，例如 `r#type` 对应 `"type"`。

提供独立的重命名属性：

```rust
#[derive(ExternalSourceEvent)]
struct MusaMallocEvent {
    #[external(rename = "process_id")]
    pid: u32,
    size: u64,
}
```

`#[serde(rename = "...")]`、`skip`、`flatten` 等 Serde 属性不影响本宏。需要同时改变两种编码结果时，由用户分别声明属性。

第一版仅支持 `external(rename = "...")`，未知 `external` 属性报编译错误。重命名后发生字段冲突也报编译错误，不能静默覆盖。

字段名作为字面 key 插入，不解析为 VRL 路径。例如重命名为 `"process.pid"` 时生成一个包含点号的 key，不自动构造嵌套对象。

## 错误设计

编码错误至少包含错误类别与字段路径：

- `IntegerOverflow`：整数超出 `i64` 范围。
- `NonFiniteFloat`：NaN 或正负无穷。
- 字段名：例如 `size`。
- 容器位置：例如 `sizes[3]` 或 `labels["device"]`。

错误路径可以内部保存为结构化 segment，仅在展示错误时格式化，避免成功路径构造错误字符串。HashMap 中多个元素同时无效时，不承诺固定的首个报错元素。

编码失败与发送失败分开处理：编码失败尚未入队；队列满、队列关闭等属于发送阶段错误。

## SourceHandle API 集成

发送入口从 `Into<Event>` 调整为 `TryInto<Event>`：

```rust
pub async fn emit<E>(&mut self, event: E) -> Result<(), EmitError>
where
    E: TryInto<Event>,
    E::Error: Into<EmitError>,
{
    let event = event.try_into().map_err(Into::into)?;

    self.sender.send_event(event).await?;

    Ok(())
}
```

以上仅展示转换与发送的连接位置。完整接口仍需执行 external source 的关闭检查、元数据填充和统计逻辑。

需要实现 `EncodeError -> EmitError`，以及对 `Infallible` 的兼容，以便原生 `Event` 和已有无损转换类型也能使用同一接口。

```rust
let mut source = runtime.source("musa_malloc")?;

source.emit(MusaMallocEvent {
    pid: 123,
    size: 4096,
}).await?;
```

宏只负责构造事件正文，不写入 source 类型、接收时间、schema、upstream ID 或 finalizer。这些信息依赖运行时上下文，由 source / SourceSender 处理。

## 批量发送

派生事件均生成 `Event::Log`。批量接口可将这些日志收集成 `Vec<LogEvent>`，然后通过 `EventArray::Logs(logs)` 直接交付 SourceSender。

宏本身不负责创建队列、缓存批次或执行异步发送。批量转换 API 应规定：整批完成转换后再入队；任意元素转换失败时，本次调用不提交任何事件，并报告批次中的元素下标。

这只保证编码阶段不发生部分提交，不代表任意后续拆批发送都具有事务性。批次大小限制和发送失败语义由批量发送 API 单独定义。

## 验证计划

- 验证所有支持的标量和容器转换结果。
- 验证 `u64` 的 `i64::MAX` 边界及溢出错误。
- 验证 NaN、正负无穷、有限浮点及空容器。
- 验证错误中的字段名、数组下标和 map key。
- 验证重命名、原始标识符、字段名冲突及字面点号 key。
- 编译测试拒绝不支持类型、嵌套容器、非具名结构体及未知属性。
- 验证类型别名、重命名 runtime 依赖和原生 Event 发送兼容性。
- 验证批量编码失败不提交任何事件。
- 基准测试比较手写原生转换、derive 转换、Serde 直接生成 Value、JSON 路径，分别观察转换吞吐和分配次数。

在获得基准结果前，仅承诺省去 JSON 和通用序列化中间层，不宣称该实现已达到实测最优性能。
