# Auron Native Python UDF Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support Spark Python UDFs in Auron's native engine by executing Python functions in-process via PyO3, eliminating JVM<->Python IPC overhead.

**Architecture:** New `auron-python-udf` Rust crate implements `SparkPythonUDFWrapperExpr` (PhysicalExpr). cloudpickle bytes are passed Scala->Rust via a new protobuf message. `AuronConverters` intercepts `EvalPythonExec` and converts it to `NativeProjectBase` containing the wrapper expr. Development happens on branch `feature/native-python-udf`.

**Tech Stack:** Rust (PyO3 0.23, Arrow 55.2.0 fork, DataFusion 49.0.0 fork), Scala (Spark 3.x), protobuf3, cloudpickle

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `Cargo.toml` (root) | Modify | Add crate member + pyo3 + auron-python-udf workspace deps |
| `native-engine/auron-python-udf/Cargo.toml` | Create | Crate metadata |
| `native-engine/auron-python-udf/src/lib.rs` | Create | Public API re-exports |
| `native-engine/auron-python-udf/src/type_convert.rs` | Create | Arrow scalar <-> Python type conversion |
| `native-engine/auron-python-udf/src/expr.rs` | Create | SparkPythonUDFWrapperExpr PhysicalExpr |
| `native-engine/auron-planner/proto/auron.proto` | Modify | Add PhysicalPythonUDFWrapperExprNode (field 10004) |
| `native-engine/auron-planner/src/planner.rs` | Modify | Add PythonUdfWrapperExpr decode branch |
| `native-engine/auron-planner/Cargo.toml` | Modify | Add auron-python-udf dep |
| `native-engine/auron-jni-bridge/src/jni_bridge.rs` | Modify | Call prepare_freethreaded_python() in INIT block |
| `native-engine/auron-jni-bridge/Cargo.toml` | Modify | Add auron-python-udf dep |
| `native-engine/auron/Cargo.toml` | Modify | Add auron-python-udf dep |
| `spark-extension/src/main/java/.../SparkAuronConfiguration.java` | Modify | Add ENABLE_PYTHON_UDF config |
| `spark-extension/src/main/scala/.../AuronConvertStrategy.scala` | Modify | Add EvalPythonExec AlwaysConvert branch |
| `spark-extension/src/main/scala/.../AuronConverters.scala` | Modify | Add convertEvalPythonExec + enablePythonUDF |
| `spark-extension/src/main/scala/.../NativeConverters.scala` | Modify | Add convertPythonUDFExpr |
| `scripts/test_python_udf.py` | Create | End-to-end validation |

---

## Task 1: Create Development Branch

- [ ] Create and switch to new branch:
```bash
cd /Users/admin/Workspaces/auron-master
git checkout -b feature/native-python-udf
```

---

## Task 2: Cargo Workspace Setup

**Files:**
- Modify: `Cargo.toml` (root, lines 19-28 members, lines 116-182 workspace.dependencies)
- Create: `native-engine/auron-python-udf/Cargo.toml`
- Modify: `native-engine/auron/Cargo.toml`

- [ ] Add to `[workspace] members` in root `Cargo.toml`:
```toml
"native-engine/auron-python-udf",
```

- [ ] Add to `[workspace.dependencies]` in root `Cargo.toml`:
```toml
auron-python-udf = { path = "./native-engine/auron-python-udf" }
pyo3 = { version = "0.23" }
```

- [ ] Create `native-engine/auron-python-udf/Cargo.toml`:
```toml
[package]
name = "auron-python-udf"
version = "0.1.0"
edition = "2024"
license = "Apache-2.0"

[dependencies]
pyo3               = { workspace = true }
arrow              = { workspace = true }
datafusion         = { workspace = true }
datafusion-ext-commons = { workspace = true }
tokio              = { workspace = true }
once_cell          = { workspace = true }
log                = { workspace = true }
```

- [ ] Add `auron-python-udf = { workspace = true }` to `[dependencies]` in `native-engine/auron/Cargo.toml`

- [ ] Create minimal `native-engine/auron-python-udf/src/lib.rs`:
```rust
pub mod expr;
pub mod type_convert;
```

- [ ] Verify workspace compiles (expect missing module errors, not workspace errors):
```bash
cargo check -p auron-python-udf 2>&1 | head -10
```

- [ ] Commit:
```bash
git add Cargo.toml native-engine/auron-python-udf/ native-engine/auron/Cargo.toml
git commit -m "build: add auron-python-udf crate to workspace"
```

---

## Task 3: Arrow<->Python Type Conversion (TDD)

**Files:**
- Create: `native-engine/auron-python-udf/src/type_convert.rs`

- [ ] Write failing tests at bottom of `type_convert.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use pyo3::Python;
    use std::sync::Arc;

    fn init() { pyo3::prepare_freethreaded_python(); }

    #[test]
    fn test_int32_roundtrip() {
        init();
        Python::with_gil(|py| {
            let arr = Arc::new(Int32Array::from(vec![Some(42)])) as ArrayRef;
            let py_val = arrow_scalar_to_py(py, &arr, 0).unwrap();
            let result = py_to_arrow_scalar(py, &py_val, &DataType::Int32).unwrap();
            assert_eq!(result, ScalarValue::Int32(Some(42)));
        });
    }

    #[test]
    fn test_null_roundtrip() {
        init();
        Python::with_gil(|py| {
            let arr = Arc::new(Int32Array::from(vec![None::<i32>])) as ArrayRef;
            let py_val = arrow_scalar_to_py(py, &arr, 0).unwrap();
            let result = py_to_arrow_scalar(py, &py_val, &DataType::Int32).unwrap();
            assert_eq!(result, ScalarValue::Int32(None));
        });
    }

    #[test]
    fn test_string_roundtrip() {
        init();
        Python::with_gil(|py| {
            let arr = Arc::new(StringArray::from(vec![Some("hello")])) as ArrayRef;
            let py_val = arrow_scalar_to_py(py, &arr, 0).unwrap();
            let result = py_to_arrow_scalar(py, &py_val, &DataType::Utf8).unwrap();
            assert_eq!(result, ScalarValue::Utf8(Some("hello".to_string())));
        });
    }
}
```

- [ ] Run tests to confirm they fail (expected: compile error, function not defined):
```bash
cargo test -p auron-python-udf 2>&1 | tail -5
```

- [ ] Implement `type_convert.rs` (arrow_scalar_to_py, py_to_arrow_scalar, build_arrow_array):
  - `arrow_scalar_to_py`: match on `array.data_type()`, downcast, return `PyObject`. Return `py.None()` if `array.is_null(row_idx)`.
  - `py_to_arrow_scalar`: match on `data_type`, `extract` from PyObject, return `ScalarValue`. Return `ScalarValue::T(None)` if `obj.is_none(py)`.
  - `build_arrow_array`: call `ScalarValue::iter_to_array(scalars.into_iter())`
  - MVP types: Int32, Int64, Float32, Float64, Utf8, LargeUtf8, Boolean, Null

- [ ] Run tests to confirm they pass:
```bash
cargo test -p auron-python-udf type_convert 2>&1 | tail -5
```
Expected: `test result: ok. 3 passed`

- [ ] Commit:
```bash
git add native-engine/auron-python-udf/src/type_convert.rs
git commit -m "feat(python-udf): implement Arrow<->Python scalar type conversion"
```

---

## Task 4: SparkPythonUDFWrapperExpr (TDD)

**Files:**
- Create: `native-engine/auron-python-udf/src/expr.rs`

Reference: `native-engine/datafusion-ext-exprs/src/spark_udf_wrapper.rs` for the PhysicalExpr pattern.

- [ ] Write failing integration test in `expr.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;
    use pyo3::Python;
    use std::sync::Arc;

    fn make_batch(values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[test]
    fn test_double_int_udf() {
        pyo3::prepare_freethreaded_python();
        let func_bytes = Python::with_gil(|py| {
            let cloudpickle = py.import("cloudpickle").unwrap();
            let code = py.eval("lambda x: x * 2", None, None).unwrap();
            cloudpickle.call_method1("dumps", (code,)).unwrap()
                .extract::<Vec<u8>>().unwrap()
        });
        let expr = SparkPythonUDFWrapperExpr::try_new(
            func_bytes, DataType::Int32, true,
            vec![Arc::new(Column::new("x", 0))],
            "lambda x: x * 2".to_string(),
        ).unwrap();
        let batch = make_batch(vec![Some(3), Some(5), None]);
        let result = expr.evaluate(&batch).unwrap().into_array(3).unwrap();
        let ints = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.value(0), 6);
        assert_eq!(ints.value(1), 10);
        assert!(ints.is_null(2));
    }
}
```

- [ ] Run test to confirm compile failure:
```bash
cargo test -p auron-python-udf test_double_int_udf 2>&1 | tail -5
```

- [ ] Implement `expr.rs` following `SparkUDFWrapperExpr` pattern:
  - Struct: `func_bytes: Vec<u8>`, `return_type`, `return_nullable`, `params: Vec<PhysicalExprRef>`, `py_func: OnceCell<PyObject>`, `expr_string`
  - `try_new` constructor
  - Implement: `PartialEq`, `DynEq`, `Hash`, `Display`, `Debug`
  - `PhysicalExpr::evaluate`: (1) eval params, (2) detect tokio context via `Handle::try_current()`, (3) wrap in `block_in_place` if in tokio context, (4) `Python::with_gil` -> `OnceCell` init callable via `cloudpickle.loads`, (5) loop rows, (6) `build_arrow_array`
  - `children()` and `with_new_children()` (copy from SparkUDFWrapperExpr pattern)

- [ ] Run test to confirm pass (requires `pip install cloudpickle`):
```bash
cargo test -p auron-python-udf test_double_int_udf 2>&1 | tail -5
```
Expected: `test result: ok. 1 passed`

- [ ] Commit:
```bash
git add native-engine/auron-python-udf/src/expr.rs
git commit -m "feat(python-udf): implement SparkPythonUDFWrapperExpr PhysicalExpr"
```

---

## Task 5: Python Interpreter Init in JNI Bridge

**Files:**
- Modify: `native-engine/auron-jni-bridge/src/jni_bridge.rs`
- Modify: `native-engine/auron-jni-bridge/Cargo.toml`

- [ ] Find the INIT block location:
```bash
grep -n "get_or_try_init\|JavaClasses\|INIT" native-engine/auron-jni-bridge/src/jni_bridge.rs | head -10
```

- [ ] Add `auron-python-udf = { workspace = true }` to `[dependencies]` in `native-engine/auron-jni-bridge/Cargo.toml`

- [ ] Inside the `INIT.get_or_try_init` closure, after `JavaClasses` initialization, add:
```rust
auron_python_udf::init_python();
```

- [ ] Add `init_python` public function to `native-engine/auron-python-udf/src/lib.rs`:
```rust
pub fn init_python() {
    pyo3::prepare_freethreaded_python();
}
```

- [ ] Verify compiles:
```bash
cargo check -p auron-jni-bridge 2>&1 | tail -5
```
Expected: no errors

- [ ] Commit:
```bash
git add native-engine/auron-jni-bridge/ native-engine/auron-python-udf/src/lib.rs
git commit -m "feat(python-udf): initialize Python interpreter in JNI startup"
```

---

## Task 6: Protobuf + Planner Decode

**Files:**
- Modify: `native-engine/auron-planner/proto/auron.proto`
- Modify: `native-engine/auron-planner/src/planner.rs`
- Modify: `native-engine/auron-planner/Cargo.toml`

- [ ] In `auron.proto`, after `get_map_value_expr = 10003;` in `PhysicalExprNode.oneof ExprType`, add:
```protobuf
// python udf wrapper
PhysicalPythonUDFWrapperExprNode python_udf_wrapper_expr = 10004;
```

- [ ] After the `PhysicalSparkUDFWrapperExprNode` message definition (~line 325), add:
```protobuf
message PhysicalPythonUDFWrapperExprNode {
  bytes func_bytes              = 1;
  ArrowType return_type         = 2;
  bool return_nullable          = 3;
  repeated PhysicalExprNode params = 4;
  string expr_string            = 5;
}
```

- [ ] Add `auron-python-udf = { workspace = true }` to `[dependencies]` in `native-engine/auron-planner/Cargo.toml`

- [ ] In `planner.rs`, after the `ExprType::SparkUdfWrapperExpr` branch (~line 949), add:
```rust
ExprType::PythonUdfWrapperExpr(e) => Arc::new(
    auron_python_udf::expr::SparkPythonUDFWrapperExpr::try_new(
        e.func_bytes.clone(),
        convert_required!(e.return_type)?,
        e.return_nullable,
        e.params
            .iter()
            .map(|x| self.try_parse_physical_expr(x, input_schema))
            .collect::<Result<Vec<_>, _>>()?,
        e.expr_string.clone(),
    )?
),
```

- [ ] Verify compiles:
```bash
cargo check -p auron-planner 2>&1 | tail -5
```
Expected: no errors

- [ ] Commit:
```bash
git add native-engine/auron-planner/
git commit -m "feat(python-udf): add PhysicalPythonUDFWrapperExprNode proto + planner decode"
```

---

## Task 7: Java Config + Scala enablePythonUDF

**Files:**
- Modify: `spark-extension/src/main/java/org/apache/auron/spark/configuration/SparkAuronConfiguration.java`
- Modify: `spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConverters.scala`

- [ ] After `ENABLE_GENERATE` in `SparkAuronConfiguration.java`, add:
```java
public static final ConfigOption<Boolean> ENABLE_PYTHON_UDF = new SQLConfOption<>(Boolean.class)
        .withKey("auron.enable.pythonUdf")
        .withCategory("Operator Supports")
        .withDescription("Enable EvalPythonExec conversion to native Python UDF execution via PyO3.")
        .withDefaultValue(true);
```

- [ ] In `AuronConverters.scala`, after `def enableGenerate`, add:
```scala
def enablePythonUDF: Boolean = SparkAuronConfiguration.ENABLE_PYTHON_UDF.get()
```

- [ ] Verify Scala compiles:
```bash
mvn compile -pl spark-extension -am -q 2>&1 | tail -10
```
Expected: BUILD SUCCESS

- [ ] Commit:
```bash
git add spark-extension/src/main/java/org/apache/auron/spark/configuration/SparkAuronConfiguration.java
git add spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConverters.scala
git commit -m "feat(python-udf): add auron.enable.pythonUdf config option"
```

---

## Task 8: Spark Integration Layer

**Files:**
- Modify: `spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConvertStrategy.scala`
- Modify: `spark-extension/src/main/scala/org/apache/spark/sql/auron/AuronConverters.scala`
- Modify: `spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeConverters.scala`

- [ ] **AuronConvertStrategy.scala**: In the second `foreachUp` loop (AlwaysConvert decisions, ~line 122), after `case e: GenerateExec if isNative(e.child)`, add:
```scala
case e: EvalPythonExec if AuronConverters.enablePythonUDF && isNative(e.child) =>
  e.setTagValue(convertStrategyTag, AlwaysConvert)
```
Add import if missing: `import org.apache.spark.sql.execution.python.EvalPythonExec`

- [ ] **NativeConverters.scala**: Add `convertPythonUDFExpr` method (near `serializeExpression`):
```scala
def convertPythonUDFExpr(
    funcBytes: Array[Byte],
    returnType: DataType,
    returnNullable: Boolean,
    params: Seq[pb.PhysicalExprNode],
    exprString: String,
): pb.PhysicalExprNode =
  pb.PhysicalExprNode.newBuilder()
    .setPythonUdfWrapperExpr(
      pb.PhysicalPythonUDFWrapperExprNode.newBuilder()
        .setFuncBytes(com.google.protobuf.ByteString.copyFrom(funcBytes))
        .setReturnType(convertDataType(returnType))
        .setReturnNullable(returnNullable)
        .addAllParams(params.asJava)
        .setExprString(exprString))
    .build()
```

- [ ] **AuronConverters.scala**: Add before `case exec: ForceNativeExecutionWrapperBase` in `convertSparkPlan`:
```scala
case e: EvalPythonExec if enablePythonUDF =>
  tryConvert(e, convertEvalPythonExec)
```

- [ ] **AuronConverters.scala**: Add `convertEvalPythonExec` and supported types:
```scala
private val supportedPythonUDFTypes: Set[DataType] = Set(
  IntegerType, LongType, FloatType, DoubleType, StringType, BooleanType, NullType,
)

def convertEvalPythonExec(exec: EvalPythonExec): Option[SparkPlan] = {
  if (!exec.udfs.forall(udf =>
      supportedPythonUDFTypes.contains(udf.dataType) &&
      udf.children.forall(e => supportedPythonUDFTypes.contains(e.dataType))))
    return None

  exec.udfs.zip(exec.resultAttrs).foreach { case (udf, resultAttr) =>
    val convertedParams = udf.children.map(NativeConverters.convertExpr)
    NativeConverters.convertPythonUDFExpr(
      udf.func.command, resultAttr.dataType, resultAttr.nullable,
      convertedParams, udf.toString,
    )
  }

  val outputExprs: Seq[NamedExpression] =
    exec.child.output.map(attr => attr: NamedExpression) ++
    exec.resultAttrs.map(attr => attr: NamedExpression)

  Some(Shims.get.newNativeProjectExec(outputExprs, exec.child))
}
```
Add required imports: `EvalPythonExec`, `IntegerType`, `LongType`, etc.

- [ ] Verify Scala compiles:
```bash
mvn compile -pl spark-extension -am -q 2>&1 | tail -10
```
Expected: BUILD SUCCESS

- [ ] Commit:
```bash
git add spark-extension/src/main/scala/org/apache/spark/sql/auron/
git commit -m "feat(python-udf): Spark integration layer for EvalPythonExec conversion"
```

---

## Task 9: Full Build + End-to-End Validation

- [ ] Confirm cloudpickle installed:
```bash
python3 -c "import cloudpickle; print(cloudpickle.__version__)"
```
If missing: `pip install cloudpickle`

- [ ] Full build:
```bash
cd /Users/admin/Workspaces/auron-master
./auron-build.sh local 2>&1 | tail -30
```
Expected: BUILD SUCCESS

- [ ] Create `scripts/test_python_udf.py`:
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, ArrayType

spark = SparkSession.builder \
    .config("spark.plugins", "org.apache.auron.plugin.AuronPlugin") \
    .config("spark.auron.enable.pythonUdf", "true") \
    .getOrCreate()

# Test 1: basic int UDF
spark.udf.register("double_it", lambda x: x * 2, IntegerType())
result = spark.range(5).selectExpr("double_it(id) as doubled").collect()
assert [r.doubled for r in result] == [0, 2, 4, 6, 8], "Test 1 failed"

# Test 2: verify NativeProject in plan (no EvalPythonExec)
plan = spark.range(5).selectExpr("double_it(id)")._jdf \
    .queryExecution().executedPlan().toString()
assert "EvalPythonExec" not in plan, f"EvalPythonExec should not appear: {plan}"

# Test 3: fallback for unsupported type (ArrayType)
spark.udf.register("wrap_array", lambda x: [x], ArrayType(IntegerType()))
spark.range(3).selectExpr("wrap_array(id)").collect()  # must not throw

print("All tests passed!")
spark.stop()
```

- [ ] Run validation:
```bash
python3 scripts/test_python_udf.py
```
Expected: `All tests passed!`

- [ ] Final commit:
```bash
git add scripts/test_python_udf.py
git commit -m "test(python-udf): add end-to-end Python UDF validation script"
```

