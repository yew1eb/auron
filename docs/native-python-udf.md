# Auron Native Python UDF 使用文档

本文档介绍如何在 Auron 中使用原生 Python UDF 支持。

---

## 概述

Spark 原生 Python UDF 通过进程间通信（Socket + pickle 序列化）与 Python Worker 交互，性能开销较大。

Auron 的原生 Python UDF 支持通过以下方式消除这一开销：

- 在 Rust 进程内使用 [PyO3](https://pyo3.rs/) 直接执行 Python 函数
- Arrow 格式数据在内存中直接转换，无需序列化
- 无 JVM↔Python 进程切换

**预期加速比：** ~3–8x（具体取决于 UDF 计算复杂度和数据规模）

---

## 前提条件

| 依赖 | 要求 |
|------|------|
| Python | 3.8+（系统需安装 Python，并可被 Auron JVM 进程找到） |
| cloudpickle | PySpark 内置，无需单独安装 |
| Auron | 包含 `native-python-udf` 特性的构建版本 |

---

## 快速开始

无需任何额外配置，功能默认开启。正常注册并使用 Python UDF 即可：

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType

spark = SparkSession.builder \
    .config("spark.plugins", "org.apache.auron.plugin.AuronPlugin") \
    .getOrCreate()

# 注册 UDF（与普通 Spark Python UDF 完全相同）
spark.udf.register("double_it", lambda x: x * 2 if x is not None else None, IntegerType())

# 使用 UDF
spark.range(10).selectExpr("double_it(id) AS doubled").show()
```

**无需修改任何现有代码。** Auron 自动在计划阶段拦截 `EvalPythonExec`，将符合条件的 UDF 替换为原生执行路径。

---

## 配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.auron.enable.pythonUdf` | `true` | 是否启用原生 Python UDF 支持 |

禁用示例：

```python
spark = SparkSession.builder \
    .config("spark.auron.enable.pythonUdf", "false") \
    .getOrCreate()
```

或通过 SparkConf 动态设置：

```python
spark.conf.set("spark.auron.enable.pythonUdf", "false")
```

---

## 支持的数据类型

原生执行路径支持以下基础类型：

| Spark SQL 类型 | Python 类型 |
|---------------|------------|
| `INT` / `INTEGER` | `int` |
| `BIGINT` / `LONG` | `int` |
| `FLOAT` | `float` |
| `DOUBLE` | `float` |
| `STRING` | `str` |
| `BOOLEAN` | `bool` |
| `NULL` | `None` |

对于**不在上述列表中的类型**（如 `ArrayType`、`MapType`、`StructType`、`DecimalType` 等），Auron 会**自动回退**到 Spark 原生 Python Worker 路径，行为与未启用 Auron 完全一致，不会报错。

---

## 注意事项

### NULL 值处理

原生执行路径会将 Arrow null 值传递为 Python `None`。请确保 UDF 逻辑正确处理 `None` 输入：

```python
# 推荐写法：显式处理 None
spark.udf.register("safe_upper", lambda s: s.upper() if s is not None else None, StringType())

# 不推荐：None.upper() 会抛出 AttributeError，导致 task 失败
spark.udf.register("unsafe_upper", lambda s: s.upper(), StringType())
```

### 当前不支持的场景（自动回退）

| 场景 | 处理方式 |
|------|---------|
| 复杂输入/输出类型（Array、Map、Struct） | 自动回退到 Python Worker |
| Pandas UDF（`@pandas_udf`） | 自动回退到 Python Worker |
| Arrow UDF | 自动回退到 Python Worker |
| UDAF（聚合 UDF） | 自动回退到 Python Worker |
| UDTF（表函数） | 自动回退到 Python Worker |

### Python GIL

当前使用 Python 3.12 及以下版本时，GIL 仍然存在。多个 Spark partition 之间并行执行，单个 partition 内的 UDF 调用串行执行。整体并发度等同于 Spark partition 数量，不会退化。

---

## 验证原生执行生效

通过 `EXPLAIN` 或 Spark UI 验证执行计划中不包含 `EvalPythonExec`：

```python
spark.udf.register("add_one", lambda x: x + 1 if x is not None else None, IntegerType())

df = spark.range(10).selectExpr("CAST(id AS INT) AS id", "add_one(CAST(id AS INT)) AS id_plus_one")

# 查看物理计划：不应出现 EvalPythonExec
df.explain(mode="formatted")
```

若计划中出现 `NativeProject` 节点而非 `EvalPythonExec`，说明原生执行已生效。

---

## 错误排查

### UDF 执行失败

Python 异常会携带完整的 traceback 抛出为 `SparkException`：

```
SparkException: Python UDF failed: ZeroDivisionError: division by zero
  File "<udf>", line 1, in <lambda>
    return x / y
```

处理方式：在 UDF 逻辑中加入防御性判断，或检查输入数据。

### Python 解释器找不到

若 Auron JVM 进程无法找到系统 Python，会在日志中输出错误。确保：
- `python3` 或 `python` 在 `PATH` 中
- `PYTHONHOME` / `PYTHONPATH` 环境变量配置正确（如使用虚拟环境）

### 强制回退到 Python Worker

如遇到意外行为，可临时禁用原生执行：

```python
spark.conf.set("spark.auron.enable.pythonUdf", "false")
```

---

## 端到端验证脚本

项目提供了一个完整的验证脚本，可在部署后运行：

```bash
python3 scripts/test_python_udf.py
```

脚本包含以下测试：
1. 基础整型 UDF（double 函数）
2. 验证执行计划不含 `EvalPythonExec`
3. 字符串 UDF
4. 不支持类型（ArrayType）自动回退验证
