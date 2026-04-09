# Auron Native Python UDF 支持 — 设计文档

**日期：** 2026-04-09
**状态：** 草稿（第二版）
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

在 Auron 的原生执行引擎中支持 Spark Python UDF，消除 JVM↔Python 进程间通信。在 Rust 进程内用 PyO3 直接执行 Python 函数，Arrow 数据在内存中直接传递，无序列化。

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
    ↕ JNI（Arrow C Data Interface FFI 指针传递）
Rust 原生执行引擎 (DataFusion fork v49.0.0 + Arrow fork v55.2.0)
```

### 现有 Scala UDF Wrapper 模式（本方案的参照）

现有 `SparkUDFWrapperExpr`（`datafusion-ext-exprs/src/spark_udf_wrapper.rs`）是一个 `PhysicalExpr`，用于在 Rust 执行引擎内调用 Spark Scala 表达式：

1. **Rust 侧**：`SparkUDFWrapperExpr` 持有序列化的 Scala 表达式字节（`serialized: Vec<u8>`），通过 `OnceCell<GlobalRef>` 懒加载 Java 上下文对象
2. **调用路径**：Rust 用 Arrow FFI（C Data Interface）传递 `RecordBatch` 指针给 Java，Java 端的 `SparkAuronUDFWrapperContext.eval(importFFIArrayPtr: Long, exportFFIArrayPtr: Long)` 执行 Spark 表达式并回传结果

### Python UDF 的差异

Python UDF 的执行需要在 Python 运行时中完成（PyO3），不需要 Java 端上下文对象。因此 `SparkPythonUDFWrapperExpr` 不依赖 Java `AuronUDFWrapperContext`，PyO3 直接在 Rust 进程内调用 Python，数据从 `RecordBatch` 直接转换为 Python 对象，无需 Arrow FFI 指针传递。

### 关键约束

Auron 使用自己 fork 的 DataFusion（`auron-project/datafusion` rev `9034aeffb`）和 Arrow（`auron-project/arrow-rs` rev `5de02520c`）。`datafusion-python` 依赖上游 DataFusion，**不能直接作为 Rust 依赖引入**（版本冲突）。本方案借鉴 `datafusion-python` 的 PyO3 UDF 模式，在 Auron 中独立实现。

---

## 3. 总体架构

### Spark 物理计划中的 Python UDF

```
EvalPythonExec(
  udfs = [PythonUDF(
    func = PythonFunction(command = <cloudpickle bytes>),
    dataType = IntegerType,
    children = [inputCol_expr]  // 输入参数表达式
  )],
  resultAttrs = [result_attr],   // 输出属性
  child = <上游 SparkPlan>
)
```

### 转换后的执行路径

```
AuronConverters.convertSparkPlan
  → case e: EvalPythonExec if enablePythonUDF
  → convertEvalPythonExec(e)
  → NativeProjectBase(
      exprs = [child输出列... , SparkPythonUDFWrapperExpr(cloudpickle_bytes, params_exprs)],
      child = ConvertToNativeBase(child)
    )
```

`SparkPythonUDFWrapperExpr` 是一个 Rust `PhysicalExpr`，在 DataFusion 执行引擎的 Project 节点中被求值：

```
DataFusion 执行引擎（Rust）
  → NativeProject 遍历 batch
  → SparkPythonUDFWrapperExpr::evaluate(batch)
      → 求值 params 表达式 → params_arrays: Vec<ArrayRef>
      → spawn_blocking { Python::with_gil(|py| {
            let func = self.py_func.get_or_try_init(|| cloudpickle.loads(func_bytes))
            for i in 0..num_rows:
                py_args = arrow_scalar_to_py(params_arrays[i])
                py_result = func.call1(py_args)
                results.push(py_to_arrow_scalar(py_result))
            build_array(results, return_type)
        }) }
      → ColumnarValue::Array(result_array)
```

### 新增组件

| 组件 | 位置 | 语言 | 说明 |
|------|------|------|------|
| `auron-python-udf` crate | `native-engine/auron-python-udf/` | Rust | `SparkPythonUDFWrapperExpr`、PyO3 调用、类型转换 |
| `AuronConverters` 扩展 | `spark-extension/.../AuronConverters.scala` | Scala | 新增 `EvalPythonExec` 转换分支 |
| `AuronConvertStrategy` 扩展 | `spark-extension/.../AuronConvertStrategy.scala` | Scala | 新增 `EvalPythonExec` 可转换性判断 |
| `NativeConverters` 扩展 | `spark-extension/.../NativeConverters.scala` | Scala | Python UDF 表达式序列化 |
| `SparkAuronConfiguration` 扩展 | `spark-extension/.../SparkAuronConfiguration.scala` | Scala | `spark.auron.enable.pythonUDF` 配置项 |

无需新增 Java JNI 方法，无需 `NativePythonUDFExec` SparkPlan，复用现有 `NativeProjectBase` 执行路径。

---

## 4. 组件详细设计

### 4.1 `auron-python-udf` Rust Crate

**加入 Cargo workspace（`Cargo.toml` 根文件）：**

```toml
[workspace]
members = [
    # ... 现有成员 ...
    "native-engine/auron-python-udf",
]

[workspace.dependencies]
# 新增
pyo3 = { version = "0.23" }
```

**目录结构：**

```
native-engine/auron-python-udf/
├── Cargo.toml
└── src/
    ├── lib.rs              # 公开接口：re-export SparkPythonUDFWrapperExpr
    ├── expr.rs             # SparkPythonUDFWrapperExpr 实现（PhysicalExpr）
    ├── type_convert.rs     # Arrow scalar ↔ Python 对象转换
    └── gil.rs              # GIL + spawn_blocking 封装
```

**`Cargo.toml`：**

```toml
[package]
name = "auron-python-udf"
edition = "2024"

[dependencies]
pyo3         = { workspace = true }
arrow        = { workspace = true }
datafusion   = { workspace = true }
datafusion-ext-commons = { workspace = true }
tokio        = { workspace = true }
once_cell    = { workspace = true }
```

**`expr.rs` — `SparkPythonUDFWrapperExpr`：**

```rust
pub struct SparkPythonUDFWrapperExpr {
    pub func_bytes: Vec<u8>,          // cloudpickle 序列化的 Python 函数
    pub return_type: DataType,
    pub return_nullable: bool,
    pub params: Vec<PhysicalExprRef>, // 输入参数表达式
    // 缓存已反序列化的 Python callable，每个 task 线程只反序列化一次
    py_func: OnceCell<Arc<Mutex<PyObject>>>,
    expr_string: String,
}

impl PhysicalExpr for SparkPythonUDFWrapperExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // 1. 求值参数表达式
        let params: Vec<ArrayRef> = self.params.iter()
            .map(|p| p.evaluate(batch).and_then(|v| v.into_array(batch.num_rows())))
            .collect::<Result<_>>()?;

        // 2. 在专用线程池（spawn_blocking）中执行 Python，避免阻塞 tokio async 线程
        let func_bytes = self.func_bytes.clone();
        let return_type = self.return_type.clone();
        let py_func = self.py_func.clone();

        let result = tokio::task::block_in_place(|| {
            Python::with_gil(|py| {
                // 3. 懒加载并缓存 Python callable
                let callable = py_func.get_or_try_init(|| -> Result<_> {
                    let cloudpickle = py.import("cloudpickle")?;
                    let func = cloudpickle.call_method1("loads", (func_bytes.as_slice(),))?;
                    Ok(Arc::new(Mutex::new(func.into())))
                })?;

                // 4. 逐行调用
                let num_rows = params[0].len();
                let mut results = Vec::with_capacity(num_rows);
                for i in 0..num_rows {
                    let py_args: Vec<PyObject> = params.iter()
                        .map(|col| arrow_scalar_to_py(py, col, i))
                        .collect::<PyResult<_>>()?;
                    let py_result = callable.lock().call1(py, PyTuple::new(py, &py_args)?)?;
                    results.push(py_to_arrow_scalar(py, &py_result, &return_type)?);
                }
                build_arrow_array(results, &return_type)
            })
        })?;

        Ok(ColumnarValue::Array(result))
    }
}
```

> **`block_in_place` vs `spawn_blocking`**：DataFusion 的执行是在 tokio 的 `block_on` 上下文中运行的，使用 `block_in_place` 允许当前线程执行阻塞操作（获取 GIL）而不阻塞整个 tokio runtime，是处理 GIL 的推荐方式。

**MVP 支持类型（`type_convert.rs`）：**

| Arrow 类型 | Python 类型 |
|-----------|------------|
| `Int32 / Int64` | `int` |
| `Float32 / Float64` | `float` |
| `Utf8 / LargeUtf8` | `str` |
| `Boolean` | `bool` |
| `Null` | `None` |

**Python 解释器初始化（`lib.rs`）：**

```rust
// 在 JNI onload 或首次使用前调用，与 Auron 现有的 INIT.get_or_try_init 机制集成
pub fn init_python() {
    pyo3::prepare_freethreaded_python();
}
```

> PyO3 0.20 起已废弃 `auto-initialize` feature，改用显式 `prepare_freethreaded_python()`。

### 4.2 Spark 集成层（Scala）

**`AuronConverters.scala` — 新增转换分支：**

```scala
// 在 convertSparkPlan 的 match 语句中新增（其他 case 之后）
case e: EvalPythonExec if enablePythonUDF =>
  tryConvert(e, convertEvalPythonExec)

// 新增转换函数
def convertEvalPythonExec(exec: EvalPythonExec): Option[SparkPlan] = {
  // 验证所有 UDF 使用 MVP 支持的类型
  if (!exec.udfs.forall(udf =>
      SupportedPythonUDFTypes.contains(udf.dataType) &&
      udf.children.forall(e => SupportedPythonUDFTypes.contains(e.dataType)))) {
    return None  // 回退到 Spark 原生路径
  }

  // 转换每个 PythonUDF 为 SparkPythonUDFWrapperExpr
  val udfExprs: Seq[(Attribute, Expression)] = exec.udfs.zip(exec.resultAttrs).map {
    case (udf, resultAttr) =>
      val convertedParams = udf.children.map(NativeConverters.convertExpr)
      val wrapperExpr = NativeConverters.serializePythonUDFExpr(
        funcBytes    = udf.func.command,
        returnType   = resultAttr.dataType,
        returnNullable = resultAttr.nullable,
        params       = convertedParams,
        exprString   = udf.toString,
      )
      (resultAttr, wrapperExpr)
  }

  // 构建输出 Project：原有输出列 + UDF 结果列
  val outputExprs: Seq[NamedExpression] =
    exec.child.output.map(attr => attr: NamedExpression) ++
    udfExprs.map { case (attr, expr) => Alias(expr, attr.name)(attr.exprId) }

  Some(Shims.get.newNativeProjectExec(outputExprs, exec.child))
}
```

**`SparkAuronConfiguration.scala` — 新增配置项：**

```scala
val ENABLE_PYTHON_UDF = conf("spark.auron.enable.pythonUDF", defaultValue = true)
```

**`AuronConvertStrategy.scala` — 新增可转换性判断：**

```scala
case e: EvalPythonExec =>
  if (AuronConverters.enablePythonUDF) markConvertible(e) else markNeverConvert(e)
```

---

## 5. 错误处理

| 层 | 错误类型 | 处理方式 |
|----|---------|---------|
| 类型检查（plan 转换时） | UDF 使用不支持的类型 | `convertEvalPythonExec` 返回 `None`，`tryConvert` 保留原 `EvalPythonExec`，Spark 走 Python Worker 路径 |
| cloudpickle 初始化 | 反序列化失败 | `OnceCell.get_or_try_init` 返回 `Err`，DataFusion task 失败，Spark 重试 task（标准 Spark 重试语义）|
| Python 执行 | UDF 函数抛出异常 | PyO3 捕获为 `PyErr`，转为 DataFusion `Error`，携带原始 Python traceback，抛出 `SparkException` |

Python traceback 透传格式：
```
SparkException: Python UDF failed: ZeroDivisionError: division by zero
  File "<udf>", line 3, in my_udf
    return x / y
Caused by: pyo3::PyErr: ZeroDivisionError
```

---

## 6. 性能分析

| 指标 | Spark 原生 Python UDF | Auron 方案 |
|------|----------------------|-----------|
| 进程切换 | 有（socket IPC） | 无 |
| 数据序列化 | pickle，逐行 | 无，Arrow array 直接转 Python 对象 |
| 并发模型 | 多 Worker 进程 | GIL 串行，partition 间并行，`block_in_place` 保护 tokio runtime |
| 预期加速比 | baseline | ~3–8x（消除进程切换 + 序列化为主） |

**GIL 处理策略：**
- Python 3.12 及以下：GIL 仍存在，使用 `tokio::task::block_in_place` 将 Python 执行卸载到独立线程，不阻塞 tokio async executor
- `gil.rs` 封装层预留 Python 3.13+ free-threaded 升级路径
- 每个 Spark partition 对应独立 task 线程，partition 间天然并行

---

## 7. 测试计划

1. **Rust 单元测试（`auron-python-udf`）：** 各基础类型的 `arrow_scalar_to_py` / `py_to_arrow_scalar` 往返正确性
2. **Rust 集成测试：** `SparkPythonUDFWrapperExpr::evaluate` 端到端（输入 RecordBatch → 输出 ArrayRef）
3. **Scala 集成测试：** 验证 `EvalPythonExec` 被 `convertEvalPythonExec` 正确替换为 `NativeProjectBase`
4. **端到端测试（Python）：** `spark.udf.register` → SQL 查询 → 结果与 Spark 原生 Python Worker 路径一致
5. **回退测试：** 使用不支持类型（如 `ArrayType`）时确认走 Spark 原生路径，无错误

---

## 8. 依赖与风险

| 项目 | 说明 | 风险 |
|------|------|------|
| PyO3 0.23 | 需加入 workspace；使用 `prepare_freethreaded_python()` 显式初始化 | 低（成熟库） |
| cloudpickle | PySpark 自带，无需额外安装 | 低 |
| `block_in_place` 语义 | DataFusion 必须在 tokio `block_on` 上下文中运行 | 中（需验证现有 tokio rt 配置） |
| GIL 争用 | partition 数量足够时 partition 间并行，单 partition 内串行可接受 | 中（MVP 接受）|
| forked DataFusion/Arrow | PyO3 直接操作 Arrow 数据，不依赖 DataFusion API，无版本冲突 | 低 |
