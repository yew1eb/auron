# Auron Native Python UDF 支持 — 设计文档

**日期：** 2026-04-09
**状态：** 草稿
**范围：** MVP — 标准 Python UDF，基础数据类型，Spark 集成

---

## 1. 背景与目标

### 问题

Spark Python UDF 通过进程间通信（socket + pickle 序列化）与 Python Worker 交互，性能极差：

```
Spark JVM → pickle row-by-row → socket → Python Worker 进程
         ← pickle row-by-row ← socket ←
```

典型开销：比原生 Scala UDF 慢 10–100x。

### 目标

在 Auron 的原生执行引擎中支持 Spark Python UDF，消除 JVM↔Python 进程间通信，用 PyO3 在 Rust 进程内直接执行 Python 函数，数据以 Arrow 格式批量传递。

### 非目标（MVP 范围外）

- Pandas UDF / Arrow UDF（独立工作项）
- UDAF（聚合 UDF）
- UDTF（表函数）
- 复杂数据类型（Array、Map、Struct）

---

## 2. 技术背景

### Auron 现有架构

```
Spark/Flink (Java/Scala)
    ↕ JNI
Rust 原生执行引擎 (DataFusion fork v49.0.0 + Arrow fork v55.2.0)
```

### 关键约束

Auron 使用自己 fork 的 DataFusion（`auron-project/datafusion` rev `9034aeffb`）和 Arrow（`auron-project/arrow-rs` rev `5de02520c`），与上游版本不同步。因此 `datafusion-python` **不能直接作为 Rust 依赖引入**（版本冲突）。

本方案借鉴 `datafusion-python` 的 PyO3 + Arrow UDF 模式，在 Auron 中独立实现，不引入 `datafusion-python` 作为依赖。

---

## 3. 总体架构

### 新执行流程

```
Spark 物理计划
  → Auron PythonUDFNativeConverter (Rule)
  → NativePythonUDFExec (SparkPlan, Scala)
  → JniBridge.callPythonUDF(funcBytes, arrowIpc, returnTypeJson)  [JNI]
  → auron-python-udf (Rust crate)
      → 解码 Arrow IPC → Vec<ArrayRef>
      → Python::with_gil(|py| { ... })
      → cloudpickle.loads(func_bytes) → Python callable
      → 逐行：Arrow scalar → Python 对象 → 调用函数 → 收集结果
      → 构建 ArrayRef 结果
  → Arrow IPC 返回 JNI
  → NativePythonUDFExec 追加结果列到 ColumnarBatch
```

### 新增组件

| 组件 | 位置 | 语言 |
|------|------|------|
| `auron-python-udf` crate | `native-engine/auron-python-udf/` | Rust |
| JNI 方法扩展 | `native-engine/auron-jni-bridge/src/jni_bridge.rs` | Rust |
| `NativePythonUDFExec` | `spark-extension/src/main/scala/.../execution/` | Scala |
| `PythonUDFNativeConverter` | `spark-extension/src/main/scala/.../rules/` | Scala |

---

## 4. 组件详细设计

### 4.1 `auron-python-udf` Rust Crate

**目录结构：**

```
native-engine/auron-python-udf/
├── Cargo.toml
└── src/
    ├── lib.rs              # 公开接口：PythonUDFExecutor
    ├── executor.rs         # 核心执行逻辑
    ├── type_convert.rs     # Arrow ↔ Python 类型转换
    └── gil.rs              # GIL 管理封装（未来 free-threaded 抽象点）
```

**核心接口：**

```rust
pub struct PythonUDFExecutor {
    func_bytes: Vec<u8>,  // cloudpickle 序列化的函数字节
}

impl PythonUDFExecutor {
    pub fn new(func_bytes: Vec<u8>) -> Self;

    /// 对输入列执行 UDF，返回结果列
    pub fn eval(
        &self,
        args: &[ArrayRef],
        return_type: &DataType,
    ) -> Result<ArrayRef>;
}
```

**执行伪代码：**

```rust
fn eval(&self, args: &[ArrayRef], return_type: &DataType) -> Result<ArrayRef> {
    Python::with_gil(|py| {
        let cloudpickle = py.import("cloudpickle")?;
        let func = cloudpickle.call_method1("loads", (&self.func_bytes,))?;

        let mut results = Vec::with_capacity(args[0].len());
        for i in 0..args[0].len() {
            let py_args: Vec<PyObject> = args.iter()
                .map(|col| arrow_scalar_to_py(py, col, i))
                .collect::<Result<_>>()?;
            let py_result = func.call1(PyTuple::new(py, &py_args))?;
            results.push(py_to_arrow_scalar(py, py_result, return_type)?);
        }
        build_array(results, return_type)
    })
}
```

**Cargo.toml 关键依赖：**

```toml
[dependencies]
pyo3        = { version = "0.23", features = ["auto-initialize"] }
arrow       = { workspace = true }
datafusion  = { workspace = true }
```

**MVP 支持类型（`type_convert.rs`）：**

| Arrow 类型 | Python 类型 |
|-----------|------------|
| `Int32 / Int64` | `int` |
| `Float32 / Float64` | `float` |
| `Utf8 / LargeUtf8` | `str` |
| `Boolean` | `bool` |
| `Null` | `None` |

### 4.2 JNI 接口扩展

**Java 侧（`JniBridge.java`）：**

```java
public static native byte[] callPythonUDF(
    byte[] funcBytes,       // cloudpickle 序列化的 Python 函数
    byte[] arrowIpcBatch,   // Arrow IPC 格式的输入列
    String returnTypeJson   // DataType JSON 序列化
);
```

**Rust 侧（`jni_bridge.rs`）：**

```rust
#[no_mangle]
pub extern "system" fn Java_org_apache_auron_jni_JniBridge_callPythonUDF(
    env: JNIEnv,
    _class: JClass,
    func_bytes: jbyteArray,
    arrow_ipc_batch: jbyteArray,
    return_type_json: JString,
) -> jbyteArray {
    let func_bytes  = env.convert_byte_array(func_bytes)?;
    let ipc_bytes   = env.convert_byte_array(arrow_ipc_batch)?;
    let type_str: String = env.get_string(return_type_json)?.into();

    let batch       = read_arrow_ipc(&ipc_bytes)?;
    let return_type = serde_json::from_str(&type_str)?;
    let executor    = PythonUDFExecutor::new(func_bytes);
    let result      = executor.eval(batch.columns(), &return_type)?;

    let result_ipc  = write_arrow_ipc_column(&result)?;
    env.byte_array_from_slice(&result_ipc)?
}
```

Arrow IPC 工具函数复用 `datafusion-ext-commons` 中现有实现。

### 4.3 Spark 集成层（Scala）

**`PythonUDFNativeConverter`（Rule）：**

```scala
case class PythonUDFNativeConverter() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan =
    plan.transformUp {
      case exec: EvalPythonExec if isSupportedByNative(exec) =>
        NativePythonUDFExec(exec.udfs, exec.resultAttrs, exec.child)
      // 不支持则保持原样，Spark 自动走 Python Worker 路径
    }

  private def isSupportedByNative(exec: EvalPythonExec): Boolean =
    exec.udfs.forall { udf =>
      SupportedTypes.mvpTypes.contains(udf.dataType) &&
      udf.children.forall(e => SupportedTypes.mvpTypes.contains(e.dataType))
    }
}
```

**`NativePythonUDFExec`（SparkPlan）：**

```scala
case class NativePythonUDFExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
) extends SparkPlan with AuronColumnarExec {

  override def doExecuteColumnar(): RDD[ColumnarBatch] =
    child.executeColumnar().mapPartitions { batches =>
      batches.map { inputBatch =>
        var outputBatch = inputBatch
        for (udf <- udfs) {
          val funcBytes  = udf.func.command
          val inputIpc   = toArrowIpc(outputBatch, udf.children)
          val returnJson = dataTypeToJson(udf.dataType)
          val resultIpc  = JniBridge.callPythonUDF(funcBytes, inputIpc, returnJson)
          val resultCol  = fromArrowIpcColumn(resultIpc)
          outputBatch    = appendColumn(outputBatch, resultCol)
        }
        outputBatch
      }
    }
}
```

**注册规则（挂入现有 `AuronSparkExtensions`）：**

```scala
extensions.injectColumnarRule(_ => PythonUDFNativeConverter())
```

---

## 5. 错误处理

| 层 | 错误类型 | 处理方式 |
|----|---------|---------|
| Python 执行 | UDF 函数抛出异常 | PyO3 捕获 → 转 Rust Error → JNI 抛 `SparkException`，携带原始 Python traceback |
| 类型转换 | 不支持的类型 | 返回 `Err`，`isSupportedByNative` 过滤，回退到 Spark 原生路径 |
| cloudpickle | 反序列化失败 | 记录错误日志，回退，不 crash |

Python traceback 透传格式：
```
SparkException: Python UDF failed: ZeroDivisionError: division by zero
  File "udf.py", line 3, in my_udf
Caused by: org.apache.auron.NativePythonUDFException
```

---

## 6. 性能分析

| 指标 | Spark 原生 Python UDF | Auron 方案 |
|------|----------------------|-----------|
| 进程切换 | 有（socket IPC） | 无 |
| 数据序列化 | pickle，逐行 | Arrow IPC，batch 级 |
| 并发模型 | 多 Worker 进程 | GIL 串行，partition 间并行 |
| 预期加速比 | baseline | ~2–5x（消除序列化为主） |

**GIL 说明：** Python 3.12 及以下版本 GIL 仍存在。每个 Spark partition 对应独立 JVM 线程，各自获取 GIL，partition 间完全并行，partition 内 UDF 串行。并发度与 Spark partition 数量一致，不退化。`gil.rs` 封装层预留 free-threaded Python（3.13+）升级路径。

---

## 7. 测试计划

1. **Rust 单元测试：** `PythonUDFExecutor` 各基础类型的输入输出正确性
2. **Scala 集成测试：** 注册 Python UDF，验证 `NativePythonUDFExec` 被正确选中
3. **端到端测试：** `spark.udf.register` → SQL 查询 → 结果与 Spark 原生路径一致
4. **回退测试：** 不支持类型时确认走 Spark 原生路径，无报错

---

## 8. 依赖与风险

| 项目 | 说明 |
|------|------|
| PyO3 0.23 | 需加入 Cargo workspace 依赖；auto-initialize 模式需系统安装 Python |
| cloudpickle | 运行环境需安装（PySpark 自带，无需额外安装） |
| Arrow IPC 工具 | 复用 `datafusion-ext-commons` 现有函数，风险低 |
| GIL 竞争 | partition 数量足够时不影响整体吞吐，MVP 可接受 |
| forked DataFusion 版本 | PyO3 不依赖 DataFusion，无版本冲突 |
