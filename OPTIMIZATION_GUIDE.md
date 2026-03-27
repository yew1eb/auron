# Apache Auron 全项目优化地图

> **版本**: v3.0
> **日期**: 2026-03-26
> **范围**: 全项目（JVM 扩展层 + JNI 边界 + Native Engine）
> **方法论**: 基于代码精确定位，所有优化点均附文件路径和行号；收益估计注明是否经过 benchmark 验证

---

## 版本历史

| 版本 | 变更说明 |
|------|---------|
| v1.0 | 初版，仅覆盖 Rust 侧，收益估计多为无依据的理论值 |
| v2.0 | 扩展至全项目（JVM + JNI + Native），修正错误优先级，区分 Bug 与性能优化 |
| v3.0 | 新增 Native 层深度优化（N-1 ~ N-16），参考 Velox / DuckDB 设计思路，精确对应代码位置 |

## 与 v1.0 的关键差异

v1.0（`native-engine/OPTIMIZATION_GUIDE.md`）的主要问题：
1. **范围不完整**：仅覆盖 Rust 侧，忽略了 JVM 数据通路、JNI 边界、Plan 转换层——这些层的 overhead 往往大于 Rust 侧的优化收益
2. **收益估计缺乏依据**：大量"30-50% 提升"无 benchmark 支撑，混淆了理论值和实测值
3. **Bug 修复与性能优化混排**：正确性问题应优先于性能优化，需单独分类
4. **部分优化脱离服务场景**：例如 Map 查找优化（`GetMapValueExpr`）在 Spark SQL 中极少触发，不应列为 P0

---

## 文档结构

```
第一部分：正确性修复（优先于所有性能优化）
第二部分：全局优先级速查表
第三部分：分模块优化详解（v2.0 原有内容）
  3.1 JVM 数据通路（Arrow IO + NativeHelper）
  3.2 JNI 边界（Plan 序列化 + Protobuf）
  3.3 计划转换层（AuronConvertStrategy + Native*Base）
  3.4 内存管理（auron-memmgr）
  3.5 Native 算子——Join
  3.6 Native 算子——聚合（Agg）
  3.7 Native 算子——Shuffle 写
  3.8 Native 算子——排序（Sort）
  3.9 Native 表达式与函数
第四部分：Native 层深度优化（v3.0 新增，参考 Velox / DuckDB）
  4.1 Hash 表架构优化
  4.2 Filter 与表达式向量化
  4.3 聚合算子深挖
  4.4 I/O 与 Parquet 层
  4.5 内存与 Spill 策略
  4.6 Sort 算子深挖
  4.7 窗口函数
  4.8 Shuffle 层
```

---

## 第一部分：正确性修复

> 这些是 **Bug**，不是优化。应在任何性能工作之前修复。

### B-1 `AccCountColumn::mem_used()` 多乘 2 倍

**文件**: `native-engine/datafusion-ext-plans/src/agg/count.rs:184`

```rust
// 错误代码
fn mem_used(&self) -> usize {
    self.values.capacity() * 2 * size_of::<i64>()  // ❌ 多乘 2
}
// 正确
fn mem_used(&self) -> usize {
    self.values.capacity() * size_of::<i64>()
}
```

**后果**：MemManager 认为内存消耗是实际的 2 倍，导致不必要的提前 spill，影响聚合性能。
**修复难度**：1 行。

---

### B-2 `MergingData::mem_used()` 多乘 2 倍

**文件**: `native-engine/datafusion-ext-plans/src/agg/agg_table.rs`

**现象**：与 B-1 类似，排序索引的内存统计偏高 2 倍。
**修复难度**：1 行。

---

### B-3 `batch_size()` const OnceCell 缓存失效

**文件**: `native-engine/datafusion-ext-commons/src/lib.rs:74`

```rust
// 错误：每次调用都创建新的 OnceCell，缓存永远不生效
pub fn batch_size() -> usize {
    const CACHED_BATCH_SIZE: OnceCell<usize> = OnceCell::new();  // ❌ const
    *CACHED_BATCH_SIZE.get_or_init(|| ...)
}
// 正确
pub fn batch_size() -> usize {
    static CACHED_BATCH_SIZE: OnceCell<usize> = OnceCell::new();  // ✅ static
    *CACHED_BATCH_SIZE.get_or_init(|| ...)
}
```

**后果**：每次调用 `batch_size()` 都重新读取配置，浪费 CPU。
**修复难度**：1 字（`const` → `static`）。

---

### B-4 `SliceAsRawBytes` 生命周期悬垂风险

**文件**: `native-engine/datafusion-ext-commons/src/lib.rs:204`

```rust
// 错误：返回的引用生命周期 'a 与 &self 完全无关，可能产生悬垂指针
pub trait SliceAsRawBytes {
    fn as_raw_bytes<'a>(&self) -> &'a [u8];  // ❌
}
// 正确：让编译器推导正确的生命周期
pub trait SliceAsRawBytes {
    fn as_raw_bytes(&self) -> &[u8];  // ✅
}
```

**后果**：潜在内存安全问题，目前可能因为调用方的使用模式而未触发，但不可依赖。
**修复难度**：低，但需要检查所有实现和调用方是否通过编译。

---

### B-5 `IpcReaderExec` 断言 panic 而非返回错误

**文件**: `native-engine/datafusion-ext-plans/src/ipc_reader_exec.rs:148`

```rust
assert!(!blocks_provider.as_obj().is_null());  // ❌ 直接 panic，无法被上层捕获
// 改为
if blocks_provider.as_obj().is_null() {
    return df_execution_err!("IpcReaderExec: blocks_provider is null");  // ✅
}
```

**后果**：当上层传入 null 时直接 panic 导致整个 Spark Task 失败，而非可重试的错误。

---

### B-6 Protobuf 递归限制的全局 unsafe hack

**文件**: `spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeRDD.scala:126-130`

```scala
// 用反射将全局 Protobuf 递归限制设为 Int.MaxValue
recursionLimitField.setInt(null, Int.MaxValue)
```

**后果**：
1. 全局状态修改，影响同 JVM 中其他 Protobuf 的使用
2. 对于真正超深的 Plan 树，仍然可能栈溢出（只是延迟了触发点）
3. 反射访问 Protobuf 内部字段，不同版本可能失效

**正确修复**：针对每次反序列化构造独立的 `CodedInputStream` 并设置递归限制：
```scala
// Protobuf Java API 中，通过 CodedInputStream 控制单次解析的递归限制
val codedInput = CodedInputStream.newInstance(bytes)
codedInput.setRecursionLimit(200)
val plan = PhysicalPlanNode.parseFrom(codedInput)
```
这样只影响当次解析，不污染全局状态。递归限制 200 应足够覆盖正常深度的 Plan 树；如果业务场景确实需要更深，可通过配置项暴露该参数。

---

## 第二部分：全局优先级速查表

> 说明：
> - 收益标注 `[实测]` 表示有 benchmark 数据，`[理论]` 表示基于代码分析的估算
> - 影响范围：`全局` = 所有查询受益；`特定` = 仅特定算子/场景

| 优先级 | 类别 | 优化点 | 影响范围 | 收益估计 | 改动量 |
|--------|------|--------|---------|---------|--------|
| **P0** | Bug | B-1~B-3 正确性修复 | 全局 | 消除不必要 spill | 极小 |
| **P0** | Bug | B-4 SliceAsRawBytes 生命周期 | 全局 | 安全性 | 小 |
| **P0** | Bug | B-6 Protobuf 递归限制 | 全局 | 稳定性 | 小 |
| **P1** | 性能 | JVM 数据通路：每行 `.copy()` | 全局 | 10-30%吞吐 [理论] | 中 |
| **P1** | 性能 | MemManager：JNI 内存查询异步化 | 全局 | 减少内存分配阻塞 | 中 |
| **P1** | 性能 | Plan 序列化：仅对无Scan查询做缓存 | 无Scan算子的查询 | O(partition) → O(1) | 小 |
| **P1** | 性能 | CachedExprsEvaluator 双重加锁 | 含表达式缓存的查询 | 减少锁竞争 | 小 |
| **P1** | 性能 | JoinHashMap SIMD 短路 | Join 密集查询 | 20-40% join probe [理论] | 小 |
| **P1** | 性能 | Shuffle RangePartitioning RowConverter 缓存 | Range Shuffle | 减少重建开销 | 小 |
| **P2** | 性能 | AuronConvertStrategy 单次树遍历 | 全局（复杂计划） | CPU 减少 | 中 |
| **P2** | 性能 | ArrowWriter 初始容量 | 含 Scan 的查询 | 减少 Arrow 向量扩容 | 小 |
| **P2** | 性能 | Plan 序列化：骨架广播（含Scan方案） | 全局（大分区数） | O(partition) → O(1) | 大 |
| **P2** | 性能 | Shuffle Vec 内存复用 | Shuffle 密集查询 | 10-20% [理论] | 中 |
| **P2** | 性能 | AggContext RowConverter Mutex→TLS | 并发聚合 | 减少锁竞争 | 中 |
| **P2** | 性能 | MemManager MIN_TRIGGER_SIZE 动态化 | 内存压力场景 | 减少不必要spill | 小 |
| **P2** | 性能 | MemManager consumers DashMap化 | 多算子并发 | 减少锁竞争 | 中 |
| **P2** | 性能 | BHJ null key 过滤避免物化 | 含null的join | 减少内存分配 | 小 |
| **P2** | 性能 | MergingData entries 内存布局压缩 | 高基数聚合 | 25% 内存节省 [理论] | 中 |
| **P3** | 性能 | JVM 数据通路：UnsafeProjection 提前初始化 | 全局 | 微小 | 小 |
| **P3** | 性能 | NativeFilterBase O(n²)→O(n) | 宽表过滤 | CPU | 小 |
| **P3** | 性能 | AggHashMap rehash SIMD | 高基数聚合 | 15-30% [理论] | 大 |
| **P3** | 性能 | GetMapValueExpr 哈希化 | map[] 访问 | 极高（场景罕见） | 大 |
| **P3** | 性能 | SortExec 类型特化 | Sort 密集查询 | 20-40% [理论] | 大 |
| **P3** | 性能 | 字符串函数批量化 | 字符串处理 | 2-4x [理论] | 大 |
| — | — | **以下为 v3.0 新增（Native 层深度优化，参考 Velox/DuckDB）** | — | — | — |
| **P1** | 性能 | N-2 Two-Level Hash Table（高基数聚合） | 高基数聚合 | L2 miss -40% [理论] | 中 |
| **P1** | 性能 | N-4 Selection Vector 下推 | Filter 密集查询 | 内存分配 -30% [理论] | 中 |
| **P1** | 性能 | N-7 Partial Agg 自适应放弃 | 高基数 agg | 无效哈希 -50% [理论] | 中 |
| **P1** | 性能 | N-10 Parquet Late Materialization 验证 | scan+filter 场景 | 3-5x [理论] | 低 |
| **P2** | 性能 | N-1 Join HashTable 自适应 Load Factor | Join 密集 | 5-10% [理论] | 低 |
| **P2** | 性能 | N-5 IsNotNull 链快路径 | 宽表 scan | 10-20% [理论] | 低 |
| **P2** | 性能 | N-6 Null-Free 快路径 | 无 null 数据 | 5-15% [理论] | 低 |
| **P2** | 性能 | N-8 列式 Accumulator 接口（Sum/Count） | Agg 密集 | 2-4x sum/count [理论] | 中 |
| **P2** | 性能 | N-9 无压缩 Arrow FFI 热路径 | I/O bound | 20-40% [理论] | 中 |
| **P2** | 性能 | N-11 全局 Spill Arbitration | 内存压力 | spill I/O 峰值降低 | 中 |
| **P2** | 性能 | N-13 数值列 MSD Radix Sort | Sort 密集 | 3-5x [理论] | 中 |
| **P2** | 性能 | N-15 Prefix Sum 加速 SUM 窗口函数 | 窗口函数 | 3-5x [理论] | 低 |
| **P2** | 性能 | N-16 Radix 预分区 Shuffle Write | Shuffle 密集 | 10-20% [理论] | 低 |
| **P3** | 性能 | N-3 SMJ SIMD Galloping | Sort-Merge Join | 1.2-3x [理论] | 高 |
| **P3** | 性能 | N-12 Spill 压缩自适应（ZSTD） | spill 密集 | spill 文件 -50% | 低 |
| **P3** | 性能 | N-14 LoserTree Branch-Free 实现 | Sort | 10-20% [理论] | 低 |

---

## 第三部分：分模块优化详解

---

### 3.1 JVM 数据通路（最关键层）

**背景**：Auron 的核心是在 Rust 侧做向量化计算，然后把结果以 Arrow 格式通过 FFI 返回 JVM。这条路上的每一次不必要拷贝都直接抵消了 Native 化的收益。

---

#### 3.1.1 UnsafeProjection 在批次循环内重建

**文件**: `spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeHelper.scala:134-137`

```scala
val rowIterator = new Iterator[InternalRow] {
  override def hasNext: Boolean = {
    // ...
    if (auronCallNativeWrapper.loadNextBatch(root => {
      if (arrowSchema == null) {
        arrowSchema = auronCallNativeWrapper.getArrowSchema
        schema = ArrowUtils.fromArrowSchema(arrowSchema)
        toUnsafe = UnsafeProjection.create(schema)  // ⚠️ 每批次触发一次
      }
```

**问题分析**：
- `UnsafeProjection.create()` 内部使用 Janino 编译 Java 字节码，宽表（50+ 列）耗时 10-50ms
- 条件 `arrowSchema == null` 只有第一批时为 true，后续批次不会重建——**这里实际上已经缓存了**
- 但如果一个 Task 的第一批次反复被重新初始化（如 barrier 场景），仍有问题

**结论**：此问题比想象中轻微，因为只触发一次。真正需要改进的是：把初始化移到 `doPrepare()` 阶段，在 Task 启动时就准备好，而不是等第一批数据到来。

**优先级**：P3（已在全局速查表中标注 P2，统一调整为 P3，因为此问题只触发一次，影响极小）。

---

#### 3.1.2 每行 `.copy()` 导致 UnsafeRow 深拷贝

**文件**: `spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeHelper.scala:142`

```scala
batchRows.append(
  ColumnarHelper
    .rootRowsIter(root)
    .map(row => toUnsafe(row).copy().asInstanceOf[InternalRow])  // ⚠️ 每行 copy
    .toSeq: _*)
```

**问题分析**：
- `UnsafeProjection` 复用底层 byte buffer，每次调用 `toUnsafe(row)` 会覆盖同一块内存
- 因此 `batchRows` 里存储的 row 必须 copy，否则所有行最终指向同一块内存
- **根本原因**：把整批 rows 存入 `ArrayBuffer[InternalRow]`，逐行消费模式导致必须 copy

**优化方向**：
- 方案 A：不把整批 rows 缓存进 ArrayBuffer，改为直接返回 `ColumnarBatch`（需要调用方支持 ColumnarBatch 接口）
- 方案 B：在 `ArrowFFIExporter` 侧维护 VectorSchemaRoot 的生命周期，确保同一批数据不被覆盖，从而不需要 copy
- 方案 C（最小改动）：改用 off-heap 的 page-based 分配，减少 GC 压力

**注意**：这个优化需要同时修改 Arrow 数据的生命周期管理，改动范围较大。
**优先级**：P1，但需要专项设计。

---

#### 3.1.3 ArrowFFIExporter 双队列线程同步

**文件**: `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/arrowio/ArrowFFIExporter.scala:69-141`

```scala
private val outputQueue: BlockingQueue[QueueState] = new ArrayBlockingQueue[QueueState](16)
private val processingQueue: BlockingQueue[Unit] = new ArrayBlockingQueue[Unit](16)
```

**架构**：一个后台线程生产 Arrow batch，主线程（Rust 回调）消费。
每批次：`outputQueue.put()` → Rust 处理 → `processingQueue.put()` → 后台线程继续。

**问题分析**：
- 每批次有 2 次阻塞操作（`put` + `take`），产生线程上下文切换
- 但这个设计是**必要的**：Rust 侧通过 JNI 调用 Java，必须有 Java 线程来驱动数据写入
- 真正的优化点：`allocator.getAllocatedMemory`（第 130 行）在写行的内层循环里调用，应提取到外层

**文件**: `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/arrowio/ArrowFFIExporter.scala:130`

```scala
// 内层循环里每行都查一次内存占用
while (rowIter.hasNext
  && allocator.getAllocatedMemory < maxBatchMemorySize   // ⚠️ 每行调用
  && arrowWriter.currentCount < maxBatchNumRows) {
  arrowWriter.write(rowIter.next())
}
```

`getAllocatedMemory` 有内部同步开销，改为每 1024 行检查一次即可。
**优先级**：P2，改动极小。

---

#### 3.1.4 ArrowWriter 初始容量硬编码为 16

**文件**: `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/arrowio/util/ArrowWriter.scala:40`

```scala
vector.setInitialCapacity(16)  // ⚠️ 批次通常是 4096-65536 行
```

**问题**：Arrow 向量默认容量 16，对于任何实际批次都需要多次扩容（每次扩容 2x），产生内存分配和拷贝。
**修复**：读取 `maxBatchNumRows` 配置作为初始容量。
**优先级**：P2，1 行改动，但需要把 `maxBatchNumRows` 传入 `ArrowWriter.create()`。

---

### 3.2 JNI 边界

---

#### 3.2.1 Plan 在每个分区重复序列化

**文件**: `auron-core/src/main/java/org/apache/auron/jni/AuronCallNativeWrapper.java`（`getRawTaskDefinition()`）

**问题**：每个分区执行时都调用 `plan.toByteArray()`。一个有 1000 个分区的查询，如果 Plan 为 500KB，则 driver 端每次 action 需要序列化 500MB 数据。

**重要约束**：`NativePlanWrapper` 中的 `nativePlan` lambda 是 `(Partition, TaskContext) => PhysicalPlanNode`，对于含 Scan 的查询，返回值包含 partition-specific 数据（文件分组、分区索引、UUID 等），**不同分区的 plan bytes 不同，不能共享缓存**。

**优化方向**：
- **方案 A（适用范围有限）**：对不含 Scan 的纯计算算子（Filter、Agg、Sort 等），`nativePlan` 结果是 partition-invariant 的，可以在 `NativePlanWrapper` 中加 `lazy val` 缓存。实施前需先判断 plan 是否不含 `NativeParquetScanBase`（或类似 scan 节点）
- **方案 B（推荐，改动较大）**：将 Plan 中不变的骨架部分（非 Scan 节点）作为广播变量，每个 Task 只序列化自己的 Scan partition 信息，在 Rust 侧合并

**优先级**：P2（原 P1 高估，因为方案 A 覆盖场景有限，方案 B 改动复杂）。

---

#### 3.2.2 MemManager 内存查询的 JNI 阻塞

**文件**: `native-engine/auron-memmgr/src/lib.rs:322-352`

```rust
// 简化示意
fn update_mem_used(&self, ...) {
    let mut status = self.status.lock().unwrap();  // 加锁
    // ... 计算
    drop(status);                                  // 释放锁

    let jvm_direct = get_mem_jvm_direct_used();    // ⚠️ JNI 调用（可能触发 JVM safepoint）
    let proc_mem = get_mem_proc_used();            // ⚠️ 系统调用

    let mut status = self.status.lock().unwrap();  // 再加锁
    // spill 决策
}
```

**问题**：
- JNI 调用 `getDirectMemoryUsed()` 可能触发 JVM safepoint，此时 Rust 线程在 safepoint 期间被挂起，而其他 Rust 线程可能在等待锁
- 每次内存分配都走这个路径，在高并发场景下成为瓶颈

**优化方案**：
- 用后台 Java 线程定期（每 100ms）更新 JVM 内存统计，Rust 侧读缓存值
- 只有在触发 spill 决策时才同步获取精确值

**优先级**：P1，影响所有高内存压力场景。

---

### 3.3 计划转换层

---

#### 3.3.1 AuronConvertStrategy 多次树遍历

**文件**: `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/AuronConvertStrategy.scala:49-190`（估计行号，需核实）

**问题**：同一棵 Plan 树被遍历多次（`foreachUp`/`foreach` 调用多次）来完成不同阶段的标记工作。

**影响分析**：
- 对于深度 30 层的典型 Join 查询（约 50-100 个节点），多次遍历 CPU 开销很小（微秒级）
- **真正影响**：如果 Plan 树反复被触发 `apply()`（如 AQE 重规划场景），累积效果明显

**优化**：合并为单次后序遍历，同时完成可转换性标记 + 子节点引用更新。
**优先级**：P2（非 P1，因为绝对耗时不大，但代码质量提升价值高）。

---

#### 3.3.2 NativeFilterBase 表达式分裂的 O(n²) 问题

**文件**: `spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/plan/NativeFilterBase.scala:68-79`

```scala
def split(expr: Expression): Unit = {
  expr match {
    case e @ And(lhs, rhs) if !isNaiveIsNotNullColumns(e) =>  // ⚠️ O(n) 递归
      split(lhs)
      split(rhs)
    case expr => splittedExprs.append(NativeConverters.convertExpr(expr))
  }
}
```

`isNaiveIsNotNullColumns(e)` 对 `AND(a, AND(b, AND(c, ...)))` 树的每个节点都被重复调用，总体 O(n²)。

**实际影响**：宽表（100 列）的 scan 通常有 100 个 `IsNotNull AND` 组成的过滤，n=100 时 O(n²) = 10000 次函数调用。这在 Plan 编译阶段发生，每次 action 一次，绝对耗时在毫秒以下，可接受。

**结论**：属于代码质量问题，不是性能瓶颈。
**优先级**：P3。

---

### 3.4 内存管理（auron-memmgr）

---

#### 3.4.1 MIN_TRIGGER_SIZE 硬编码不自适应

**文件**: `native-engine/auron-memmgr/src/lib.rs`（MIN_TRIGGER_SIZE = 16MB）

**问题**：16MB 的 spill 触发阈值在不同查询规模下行为差异大：
- 小查询（单 Executor 1GB 内存）：16MB 可能过大，导致 OOM 前没有及时 spill
- 大查询（100GB 数据）：16MB 过小，导致 spill 过于频繁

**优化**：将触发阈值改为动态：`MIN_TRIGGER_SIZE = max(16MB, total_memory * 0.01)`
**优先级**：P2。

---

#### 3.4.2 consumers 列表遍历的 Mutex 竞争

**文件**: `native-engine/auron-memmgr/src/lib.rs`

**问题**：每次内存更新都需要遍历 `consumers: Mutex<Vec<Arc<MemConsumerInfo>>>` 来找到目标 consumer。

**优化**：改为 `DashMap<ConsumerId, MemConsumerInfo>`，减少全局锁持有时间。
**注意**：`DashMap` 目前不是 `auron-memmgr` 的依赖，需要在 `Cargo.toml` 中添加 `dashmap` crate。
**优先级**：P2，主要影响高并发（多算子并行执行）场景。

---

### 3.5 Native 算子——Join

---

#### 3.5.1 JoinHashMap Probe 的 SIMD 短路

**文件**: `native-engine/datafusion-ext-plans/src/joins/join_hash_map.rs:255`（估计行号）

```rust
// 当前：两次 SIMD 比较都完整执行
let hash_matched = self.map[e].hashes.simd_eq(Simd::splat(hashes[i]));
let empty = self.map[e].hashes.simd_eq(Simd::splat(0));  // 总是执行
if let Some(pos) = (hash_matched | empty).first_set() { ... }
```

**正确优化思路**：
```rust
// 先检查 hash_matched（命中路径更常见）
let hash_matched = self.map[e].hashes.simd_eq(Simd::splat(hashes[i]));
if let Some(pos) = hash_matched.first_set() {
    return self.map[e].values[pos]; // 命中
}
// 再检查是否遇到空槽（未命中路径）
let empty = self.map[e].hashes.simd_eq(Simd::splat(0));
if empty.any() {
    return MapValue::EMPTY;
}
// 否则继续探测下一个 slot
```

**收益**：在 hash 命中率高时（典型 join 场景），减少约 50% 的 SIMD 指令。在 hash miss 率高时（高基数 join）收益较小。
**注意**：需要 benchmark 验证实际收益，因为现代 CPU 可以 speculative 执行两条 SIMD 指令。
**优先级**：P1，改动小，理论收益明确。

---

#### 3.5.2 BHJ null key 过滤

**文件**: `native-engine/datafusion-ext-plans/src/joins/bhj/full_join.rs`

**问题**：Build side null key 过滤有一次额外的 `.collect()` 调用，将流式数据物化到内存。

**优化**：改为 filter 迭代器，避免中间物化。
**优先级**：P2，影响含 null 值的 join 场景。

---

### 3.6 Native 算子——聚合

---

#### 3.6.1 CachedExprsEvaluator 双重加锁

**文件**: `native-engine/datafusion-ext-plans/src/cached_exprs_evaluator.rs:423`

```rust
fn get(&self, id: usize, ...) -> Result<ColumnarValue> {
    if let Some(cached) = &self.values.lock()[id] {  // 锁 #1：检查
        return Ok(cached.clone());
    }
    let cached = evaluate_on_vacant()?;
    self.values.lock()[id] = Some(cached.clone());    // 锁 #2：写入
    Ok(cached)
}
```

**问题**：两次加锁之间有窗口期，可能导致同一个表达式被重复计算（但不会写入错误结果——两次计算结果相同，只是浪费 CPU）。这是**性能问题**而非数据正确性问题。
**优化**：合并为单次加锁操作，或改用 `entry` API。
**优先级**：P1（性能）。

---

#### 3.6.2 AggContext RowConverter 全局 Mutex

**文件**: `native-engine/datafusion-ext-plans/src/agg/agg_ctx.rs:241`

```rust
self.grouping_row_converter.lock().convert_columns(...)
```

**问题**：多个并发线程处理不同分区时，都竞争同一个 `grouping_row_converter` 的 Mutex。

**优化**：`RowConverter` 的状态是否可以 per-thread？如果可以，改为 `thread_local!` 存储。需要验证 DataFusion RowConverter 的 `Send + Sync` 语义。
**优先级**：P2，主要影响高并发聚合场景（多核利用率高时才有明显竞争）。

---

#### 3.6.3 AggHashMap rehash 的 SIMD 优化空间

**文件**: `native-engine/datafusion-ext-plans/src/agg/agg_hash_map.rs`

**现状**：rehash 过程中的数据迁移仍有标量路径。
**优化**：使用 SIMD gather/scatter 指令批量迁移哈希槽。
**注意**：这是高难度优化，且 rehash 只在哈希表扩容时发生（不是每批次），实际影响有限。
**优先级**：P3（原 P1 高估了收益）。

---

#### 3.6.4 MergingData entries 内存布局

**文件**: `native-engine/datafusion-ext-plans/src/agg/agg_table.rs`

**当前**：`entries: Vec<(u32, u32, u32, u32)>`（16 字节）
**优化**：第一个字段 bucket_id 是否可以用 `u16`？需要验证 bucket 数量上限。
**预期收益**：12 字节 vs 16 字节 = 25% 内存节省，对 L1/L2 缓存命中率有改善。
**优先级**：P2，需要先验证 bucket 数量约束。

---

### 3.7 Native 算子——Shuffle 写

---

#### 3.7.1 Shuffle Vec 重复分配

**文件**: `native-engine/datafusion-ext-plans/src/shuffle/buffered_data.rs`

**问题**：每次 `sort_batches_by_partition_id()` 都 `collect::<Vec<_>>()`，频繁分配和释放。
**优化**：在 `SortShuffleRepartitioner` 级别维护复用缓冲区，调用前 `clear()` 而非重新分配。
**优先级**：P2，shuffle 密集场景下有明显改善。

---

#### 3.7.2 RangePartitioning 每批重建 RowConverter

**文件**: `native-engine/datafusion-ext-plans/src/shuffle/mod.rs`

**问题**：range partition 的 boundary 计算中，`RowConverter` 在每批数据处理时重建，而它的 schema 在整个 Stage 内不变。
**优化**：将 `RowConverter` 提升为 `ShuffleSortPartitioner` 的字段，初始化一次。
**优先级**：P1，改动小，收益明确。

---

### 3.8 Native 算子——排序

---

#### 3.8.1 SortExec 缺乏原始类型特化

**文件**: `native-engine/datafusion-ext-plans/src/sort_exec.rs`

**现状**：排序使用 `RowConverter` 将列式数据转为行式 bytes 再排序。对于 `ORDER BY int_col` 这种单列整数排序，有极大的优化空间。

**优化**：为 `Int32/Int64/Float64` 单列排序实现特化路径，直接用原生类型比较，避免 row bytes 转换。

**收益**：理论上 2-4x 提升，但实际受益取决于查询是否 sort-heavy。
**优先级**：P3，改动量大，且 sort 通常不是 Spark SQL 的主要瓶颈（shuffle 通常更瓶颈）。

---

### 3.9 Native 表达式与函数

---

#### 3.9.1 日期函数重复 cast

**文件**: `native-engine/datafusion-ext-functions/src/spark_dates.rs`

```rust
// spark_year, spark_month, spark_day 都各自 cast 一次
pub fn spark_year(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;  // 重复
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Year)?))
}
```

**优化**：提取公共 cast 逻辑，或在表达式树层面做 CSE（公共子表达式消除）。
**优先级**：P3，日期函数不是典型瓶颈。

---

#### 3.9.2 字符串函数逐行处理

**文件**: `native-engine/datafusion-ext-functions/src/spark_strings.rs`

**现状**：字符串函数（`like`、`substring`、`trim` 等）没有 SIMD 批量路径。
**优化**：对高频字符串操作使用 `memchr` crate 或 SIMD 实现。
**注意**：字符串处理的 SIMD 化相当复杂（UTF-8 边界问题），且 DataFusion 上游也在持续改进此类函数。建议先等待上游，而非自行实现。
**优先级**：P3，且建议跟踪上游而非自实现。

---

## 附录：被降级的优化点说明

以下是 v1.0 中标为 P0/P1 但经本次分析后降级的条目：

| v1.0 条目 | 降级理由 |
|-----------|---------|
| `GetMapValueExpr` Map 线性搜索（原 P0） | `map['key']` 访问在典型 Spark SQL 中极少见，应为 P3 |
| `AggHashMap rehash` SIMD（原 P1） | rehash 仅在扩容时发生，非热路径，降为 P3 |
| 无锁 RowConverter 30-50% 并发聚合提升 | 收益估计过高，实际竞争程度取决于并发度，需要 benchmark |
| `SortExec` 类型特化（原 P1） | Sort 通常不是 Spark 主瓶颈，改动量大，降为 P3 |
| JNI 抽象层（原架构优化） | 属于工程质量改进，非性能优化，不在本文档范围 |
| 配置集中化（原架构优化） | 同上 |

---

*文档版本: v2.0 | 范围: 全项目 | 方法: 代码精确分析（已被 v3.0 内容追加，见下方第四部分）*

---

## 第四部分：Native 层深度优化（v3.0 新增）

> **参考来源**：Meta Velox（C++ 向量化执行引擎）和 DuckDB（嵌入式分析数据库）的算子与表达式层核心设计思路。
> 本部分所有优化点（N-1 ~ N-16）均为 v2.0 未覆盖的新方向，基于对 Auron 当前代码的精确分析。

---

### 4.1 Hash 表架构优化

---

#### N-1 Join HashTable 自适应 Load Factor

**文件**: `native-engine/datafusion-ext-plans/src/joins/join_hash_map.rs:159`

**当前**：`map_mod_bits` 计算时固定用 `num_items * 2 / GROUP_SIZE`，即 50% 的 load factor。

**问题**：50% 是一个保守的固定值。若 key 分布均匀（理想哈希），实际可以用更高 load factor（65-70%）减少内存；若 key 分布极不均匀，50% 也可能导致 probe 步长变长。

**优化**：在 BHJ build 完成后，采样 probe 平均步长作为反馈信号：
- 平均步长 < 1.1：当前 load factor 可以适当提高（内存节省）
- 平均步长 > 1.5：说明碰撞严重，下次 build 时降低 load factor

对于重复执行的查询（Spark 的 Broadcast Join 会在每个 executor 上 build 一次），这个自适应可以在多个 task 之间传播统计信息。

**优先级**：P2。改动量小，收益 5-10%（内存和 probe 速度）[理论]。

---

#### N-2 Two-Level Hash Table（高基数聚合）

**文件**: `native-engine/datafusion-ext-plans/src/agg/agg_hash_map.rs`

**Velox 的设计**：`HashAggregation` 对高基数数据使用两阶段哈希表：
1. **L1 小表**（fits L1/L2 cache，约 64KB = ~4000 entries）：处理每个 batch 的数据
2. **L2 分区表**：按 hash 高位 bits 分成 N 个分区（N 通常为 CPU 数量），L1 满时按分区 flush 到 L2

**Auron 现状**：`AggHashMap` 是单一大哈希表，随数据增长不断 rehash，整个表无法 fit cache 时 L2 miss 率飙升。

**优化方向**：
```
阶段 1：每个 batch 先进入小表（固定 4096 slots），overflow 行批量 flush
阶段 2：flush 时按 hash >> (32-LOG_N) 分桶写入 N 个分区表
最终 merge：从各分区表合并最终结果
```

**关键收益点**：小表永远 fit cache，99% 的 upsert 操作在 L1 cache 内完成。

**落点**：`agg_hash_map.rs` + `agg_table.rs` 的 `OwnedKey` 管理。

**优先级**：P1。高基数聚合（GROUP BY user_id on 10亿行）场景，L2 miss 减少 40-60% [理论]。改动量中等，需要专项设计。

---

#### N-3 Sort-Merge Join SIMD Galloping

**文件**: `native-engine/datafusion-ext-plans/src/joins/smj/full_join.rs`

**DuckDB 的做法**：当 SMJ 的一侧出现连续相同 key（例如 `ORDER BY + GROUP BY` 后的数据），用 **SIMD 比较批量跳过**相同 key 块，避免逐行比较。

**Auron 现状**：SMJ merge 循环基于 `LoserTree` 逐行推进，没有针对 key 重复的快路径。

**优化**：在 advance 逻辑中，当检测到当前 key 等于前一个 key 时，切换到 galloping 模式：
- 用 `2^k` 步跳跃查找边界，找到后用 binary search 精确定位
- 整个过程可以 SIMD 化（比较 key 的 bytes）

**优先级**：P3。实现复杂，仅在 key 重复率高的 SMJ 场景有明显收益（1.2-3x）[理论]。

---

### 4.2 Filter 与表达式向量化

---

#### N-4 Selection Vector 下推

**文件**: `native-engine/datafusion-ext-plans/src/filter_exec.rs:200-224`

**DuckDB 的核心设计**：所有算子不物化过滤后的 RecordBatch，而是传递一个 **SelectionVector**（`uint16` 数组，存活行的下标）。只有在必须输出时（如写入 shuffle buffer）才做物化（`take` 操作）。

**Auron 现状**：`execute_filter` 最终调用 DataFusion 的 `filter_record_batch`，这会**分配新内存 + 拷贝数据**（Arrow `filter` kernel 的行为）。

**影响分析**：
- 一个 batch 10000 行，filter 后剩 100 行（1% selectivity），当前会分配 100 行的新 RecordBatch
- 这 100 行可能马上进入下一个 `ProjectExec` 又被重新处理
- 整条链路产生多次不必要的内存分配

**优化方向**：
```rust
// 扩展 ExecuteWithColumnPruning 的思路，增加 SelectionVector 传递接口
trait ExecuteWithSelection {
    fn execute_with_selection(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        selection: Option<SelectionVector>,
    ) -> Result<SendableRecordBatchStream>;
}
```

FilterExec 只输出 selection vector，ProjectExec/AggExec 直接消费，避免中间物化。

**落点**：`filter_exec.rs` + `cached_exprs_evaluator.rs` + `project_exec.rs`

**优先级**：P1。高 selectivity filter 场景内存分配减少 30-50% [理论]。需要专项设计，参考 DuckDB 的 Vector 接口设计。

---

#### N-5 IsNotNull 密集链快路径

**文件**: `native-engine/datafusion-ext-plans/src/common/cached_exprs_evaluator.rs`

**问题背景**：Spark 的 Parquet scan 通常生成大量 `IsNotNull(col)` 谓词（每个 non-nullable 列一个），最终在 `FilterExec` 中形成深度 AND 树，例如 100 列宽表 = 100 个 `IsNotNull` 表达式。

**优化**：在 `CachedExprsEvaluator::filter()` 前增加快路径分析：
1. 检查 predicates 中的 `IsNotNull(Column)` 节点，收集涉及的列 index 集合
2. 对这些列调用 `column.null_count() == 0` 快速判断——如果为 0，整批数据该谓词自动通过
3. 当所有 IsNotNull 谓词通过时，跳过这些谓词的 evaluate，仅执行剩余的业务谓词

**Velox 类比**：Velox 的 `ExprSet::eval()` 有类似的 `isNullFreeColumn` 快路径。

**优先级**：P2。宽表（50+ 列）scan 场景，10-20% [理论]。改动量小，且风险极低。

---

#### N-6 Null-Free 批次快路径

**文件**: `native-engine/datafusion-ext-plans/src/common/cached_exprs_evaluator.rs`

**DuckDB 的设计**：每个 Vector 维护一个 `validity mask`（64-bit 对齐 bitset），当 `null_count == 0` 时跳过所有 null 检查分支，走无 null 检查的特化计算路径。

**Auron 现状**：Arrow 的 `ArrayRef` 已有 `null_count()` 接口，但各表达式实现没有利用这个信息走快路径。

**优化**：
```rust
// 在 filter 入口处检查整个 batch 是否无 null
let is_null_free = batch.columns().iter().all(|c| c.null_count() == 0);
if is_null_free {
    // 走无 null 检查的特化 evaluate 路径（可以用 unsafe 跳过 validity bit 检查）
}
```

对于数仓场景（数据通常无 null，null 是异常值），这个快路径几乎总是生效。

**优先级**：P2。无 null 数据场景（典型数仓）5-15% [理论]。改动量极小。

---

### 4.3 聚合算子深挖

---

#### N-7 Partial Agg 自适应放弃（Adaptive Partial Aggregation）

**文件**: `native-engine/datafusion-ext-plans/src/agg_exec.rs`

**Velox 的三模式设计**：
1. **Partial 模式（默认）**：哈希聚合，合并相同 key 减少数据量
2. **Streaming 模式**：输入已按 GROUP BY key 排序时，直接流式聚合
3. **放弃模式（Abandon）**：当检测到 `去重 key 数 / 总行数 > 阈值`（例如 0.8），说明数据基数极高，partial agg 几乎没有收益，直接 pass-through 到 shuffle，让 final agg 处理

**Auron 现状**：`agg_exec.rs` 只有一种聚合模式，高基数场景下做了大量无效的哈希计算。

**优化实现**：
```rust
// 在 agg_table.rs 的 insert_batch 中，每处理 N 行后采样
let reduction_ratio = hash_table.len() as f64 / total_rows_processed as f64;
if reduction_ratio > ABANDON_THRESHOLD {  // 默认 0.8
    // 切换到 pass-through 模式，后续 batch 直接转发不聚合
    self.mode = AggMode::PassThrough;
}
```

**落点**：`agg_exec.rs`（增加 mode 状态机）+ `agg_table.rs`（增加 pass-through 分支）

**优先级**：P1。高基数 GROUP BY 场景减少无效哈希计算 50%+ [理论]。这是 Velox 经过大规模生产验证的优化。

---

#### N-8 列式 Accumulator 接口

**文件**: `native-engine/datafusion-ext-plans/src/agg/acc.rs`

**DuckDB 的设计**：Aggregation 的 accumulator 直接对整列数据（`ArrayRef`）做批量操作，而非逐行 update。

**Auron 现状**：`AccumTrait` 的 `update_batch` 接口接收 `&[ArrayRef]` 和 row indices，内部仍是 per-row 循环（以 `sum.rs` 为例，遍历 `indices` 逐个累加）。

**优化**：对数值型聚合函数增加 columnar 特化接口：
```rust
// 新增可选的列式批量接口
fn update_columnar_unchecked(&mut self, column: &ArrayRef, indices: &UInt32Array) {
    // 利用 Arrow compute 的 sum_kernel / min_kernel
    // 或直接用 SIMD 对 primitive array 求和
}
```

对 `SUM(Int64)`，整列求和可以用 `std::simd` 的水平加法，比逐行循环快 4-8x。

**落点**：`agg/acc.rs`（接口扩展）+ `agg/sum.rs`、`agg/count.rs`、`agg/avg.rs`、`agg/maxmin.rs`（实现）

**优先级**：P2。Sum/Count/Min/Max 的列式加速，数值聚合 2-4x [理论]。

---

### 4.4 I/O 与 Parquet 层

---

#### N-9 无压缩 Arrow FFI 热路径

**文件**: `native-engine/datafusion-ext-plans/src/ipc_writer_exec.rs` + `ipc_reader_exec.rs`

**当前**：Rust 算子间通过 LZ4 压缩的 IPC 格式传递数据（`SpillCompressedWriter`），即使是同机器同进程内的算子间传输也走压缩路径。

**优化**：增加配置项 `auron.ipc.compress_threshold`，当 RecordBatch 大小低于阈值时，走无压缩的 Arrow FFI 路径（直接传递 `ArrowArray` C Data Interface 指针，零拷贝）：
- 同机器算子间：无压缩（避免 LZ4 encode/decode CPU 开销）
- 跨节点 shuffle 写入：保留 LZ4（网络 I/O bound）

**前提**：需要评估 Arrow buffer 的生命周期管理（Rust 侧 buffer 在 JVM 侧消费完前不能释放）。Auron 已有 `ArrowFFIExporter` 的基础，扩展方向明确。

**优先级**：P2。I/O bound 场景（算子 pipeline 中的中间数据）20-40% [理论]。

---

#### N-10 Parquet Late Materialization 验证与强化

**文件**: `native-engine/datafusion-ext-plans/src/parquet_exec.rs`

**DuckDB 的核心优化**：Parquet 读取时先只解码 filter 涉及的列，过滤后再解码其余列（Late Materialization）。对宽表 + 高 selectivity filter，效果 3-5x。

**DataFusion 49.x 现状**：DataFusion 已支持 `RowFilter`（谓词下推到 Parquet reader），但是否充分利用了 page-level pruning + dict encoding filter 需要验证。

**验证步骤**：
1. 检查 `parquet_exec.rs` 中是否将 `FilterExec` 的谓词注册为 Parquet `RowFilter`
2. 检查是否开启了 `ParquetRecordBatchReaderBuilder::with_row_filter()` 的优化选项
3. 对于 string 等值过滤（`col = 'value'`），验证是否走了 Parquet dict page 的直接 bitmap filter

**落点**：`parquet_exec.rs` + Spark 侧的 `NativeParquetScanBase` 谓词序列化

**优先级**：P1。可能是低改动量高收益的优化（只需验证接口对接，不需要自己实现 Late Materialization）。

---

### 4.5 内存与 Spill 策略

---

#### N-11 全局 Spill Arbitration

**文件**: `native-engine/auron-memmgr/src/lib.rs`

**Velox 的 `MemoryArbitrator` 设计**：内存不足时，由全局仲裁者**主动挑选**当前内存占用最大的 operator 触发 spill，而非让每个 operator 独立轮询。

**Auron 现状**：每个 `MemConsumer` 在 `update_mem_used` 时独立判断是否需要 spill（基于全局内存水位）。在多个算子并发时，可能同时触发 spill，造成 I/O 峰值。

**优化**：在 `MemManager` 中增加 `spill_largest_consumer()` 方法：
```rust
// 当全局内存超过 high watermark 时，仲裁者主动挑最大的 consumer
fn spill_largest_consumer(&self) -> Result<bool> {
    let largest = self.consumers
        .lock()
        .iter()
        .max_by_key(|c| c.mem_used());
    // 通知该 consumer 触发 spill
}
```

这样保证任意时刻只有一个 consumer 在 spill，避免并发 spill 的 I/O 争用。

**优先级**：P2。内存压力场景，spill I/O 峰值降低，整体吞吐提升。

---

#### N-12 Spill 压缩算法自适应

**文件**: `native-engine/auron-memmgr/src/spill.rs`

**当前**：Spill 固定使用 LZ4（压缩率约 2-3x，解压极快）。

**问题**：当内存极度紧张时，更高压缩率（ZSTD 约 4-6x）可以减少磁盘 I/O，让 spill/restore 整体更快——因为瓶颈从 CPU（LZ4 已足够快）变成了磁盘 I/O。

**优化**：
```rust
// 根据 spill_pressure 选择算法
let codec = match spill_pressure {
    p if p > 0.9 => Codec::Zstd(3),   // 高压力：最大化压缩率
    _ => Codec::Lz4,                   // 正常：最大化速度
};
```

通过配置项 `auron.spill.compression=auto|lz4|zstd` 控制，`auto` 为默认。

**优先级**：P3。改动量小，但 spill 密集场景效果明显（文件体积 -50%）[理论]。

---

### 4.6 Sort 算子深挖

---

#### N-13 数值列 MSD Radix Sort

**文件**: `native-engine/datafusion-ext-plans/src/sort_exec.rs`

**DuckDB 和 Velox 的做法**：对数值类型（Int32/Int64/Float64）的单列排序，用 **MSD Radix Sort（8-pass，每次 8 bits）** 替代基于 comparison 的 pdqsort。

**Auron 现状**：`sort_exec.rs` 将所有排序列通过 `RowConverter` 转换为 byte 序列后排序，对 `ORDER BY int_col` 这种简单场景有巨大的过度开销（类型转换 + bytes 比较 vs 直接整数比较）。

**优化路径**：
1. 在 `sort_exec.rs` 的 `execute` 入口检测排序 key 类型
2. 如果第一个 key 是 `Int32/Int64/UInt32/UInt64/Float32/Float64` 且无 null，走特化路径：
   - 提取该列为 `PrimitiveArray`
   - 用 Radix Sort 排序，得到排序后的 row indices
   - 其他列根据 indices 重排（`take` 操作）
3. 处理 Float 的 NaN 和 null 的边界情况

**收益**：数值单列排序 3-5x [理论]。Radix sort 对于 `i64` 是稳定的 O(n) 算法，对大数据量远优于 O(n log n) 的比较排序。

**优先级**：P2（v2.0 中 `SortExec` 类型特化被降为 P3，但考虑到 Radix Sort 的实现难度远低于全类型特化，重新评级为 P2）。

---

#### N-14 LoserTree Branch-Free 实现

**文件**: `native-engine/datafusion-ext-commons/src/algorithm/loser_tree.rs`

**问题**：LoserTree 的比较操作在 tight merge 循环中，分支预测的正确率较低（winner 轮流交替），产生较多的 branch misprediction penalty（现代 CPU 约 15-20 cycles per miss）。

**优化**：对固定大小类型（Int32/Int64 key）的比较，用 **branch-free select** 实现：
```rust
// 用 cmov 替代 if/else，避免分支预测
#[inline]
fn min_idx_branchfree(a: i64, b: i64, idx_a: u32, idx_b: u32) -> u32 {
    let pick_a = (a <= b) as u32;
    pick_a * idx_a + (1 - pick_a) * idx_b
}
```

Rust 编译器在 `#[inline]` + 优化级别 O3 下，对 `if a <= b { a } else { b }` 通常已能生成 `cmov`，但对于复杂的 key bytes 比较不一定能。

**优先级**：P3。改动量小，收益 10-20% [理论]，仅对 merge sort 热路径有效。

---

### 4.7 窗口函数

---

#### N-15 Prefix Sum 加速 SUM/AVG 窗口函数

**文件**: `native-engine/datafusion-ext-plans/src/window/processors/agg_processor.rs`

**当前**：窗口聚合（`SUM(x) OVER (PARTITION BY p ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`）是逐行累积，每行更新一次 accumulator。

**优化**：对 `SUM/AVG/COUNT` 窗口函数 + `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`（最常见的窗口 frame），用 **prefix sum** 一次性计算整个分区的结果：

```rust
// 对整列做 prefix sum（完全向量化，可 SIMD 加速）
fn prefix_sum(values: &Float64Array) -> Float64Array {
    let mut acc = 0.0f64;
    values.iter().map(|v| {
        acc += v.unwrap_or(0.0);
        acc
    }).collect()
}
```

Prefix sum 可以用 SIMD 实现（Intel 的 `_mm256_add_ps` 的级联前缀求和），但即使是标量实现也比当前的逐行 accumulator 快，因为避免了虚函数调用和 trait dispatch。

**适用范围**：`UNBOUNDED PRECEDING AND CURRENT ROW` frame（最常见）+ 数值聚合函数（SUM/AVG/COUNT）

**优先级**：P2。窗口函数密集场景 3-5x [理论]。改动量小，风险低。

---

### 4.8 Shuffle 层新思路

---

#### N-16 Radix 预分区加速 Shuffle Write

**文件**: `native-engine/datafusion-ext-plans/src/shuffle/sort_repartitioner.rs`

**当前**：`sort_batches_by_partition_id()` 的核心是对行按 partition_id 排序，底层用 Arrow 的 `RowConverter` + 标准排序（pdqsort）。

**DuckDB 的 RadixPartition 思路**：由于 partition_id 是有界整数（值域 [0, num_partitions)），可以用 **Counting Sort（计数排序）** 代替比较排序：
1. 第一遍扫描：统计每个 partition_id 的行数 → O(n)
2. 计算各 partition 的起始偏移 → O(p)（p = partition 数量）
3. 第二遍扫描：将行直接写入各 partition 的目标位置 → O(n)

整体 O(n + p)，远优于 O(n log n) 的比较排序，且完全无分支、cache friendly。

**适用条件**：partition_id 列已提前计算好（Auron 的 shuffle 执行流程中 partition_id 在 batch 内就已确定）。

**落点**：`shuffle/sort_repartitioner.rs` 的 `sort_batches_by_partition_id()` 方法，以及 `shuffle/buffered_data.rs`

**优先级**：P2。改动量小，收益 10-20% [理论]，对大 partition 数（>1000）场景更明显。

---

## 附录 B：新增优化点快速索引（v3.0）

| ID | 优化点 | 参考来源 | 落点文件 | 优先级 |
|----|--------|---------|---------|--------|
| N-1 | Join HashTable 自适应 Load Factor | DuckDB | `joins/join_hash_map.rs` | P2 |
| N-2 | Two-Level Hash Table（高基数聚合） | Velox | `agg/agg_hash_map.rs` | P1 |
| N-3 | Sort-Merge Join SIMD Galloping | DuckDB | `joins/smj/full_join.rs` | P3 |
| N-4 | Selection Vector 下推 | DuckDB | `filter_exec.rs` | P1 |
| N-5 | IsNotNull 密集链快路径 | Velox | `common/cached_exprs_evaluator.rs` | P2 |
| N-6 | Null-Free 批次快路径 | DuckDB | `common/cached_exprs_evaluator.rs` | P2 |
| N-7 | Partial Agg 自适应放弃 | Velox | `agg_exec.rs` | P1 |
| N-8 | 列式 Accumulator 接口 | DuckDB | `agg/acc.rs` + `agg/sum.rs` 等 | P2 |
| N-9 | 无压缩 Arrow FFI 热路径 | 通用 | `ipc_writer_exec.rs` + `ipc_reader_exec.rs` | P2 |
| N-10 | Parquet Late Materialization 验证 | DuckDB | `parquet_exec.rs` | P1 |
| N-11 | 全局 Spill Arbitration | Velox | `auron-memmgr/src/lib.rs` | P2 |
| N-12 | Spill 压缩算法自适应（ZSTD） | 通用 | `auron-memmgr/src/spill.rs` | P3 |
| N-13 | 数值列 MSD Radix Sort | DuckDB/Velox | `sort_exec.rs` | P2 |
| N-14 | LoserTree Branch-Free 实现 | 通用 | `datafusion-ext-commons/algorithm/loser_tree.rs` | P3 |
| N-15 | Prefix Sum 加速 SUM 窗口函数 | Velox | `window/processors/agg_processor.rs` | P2 |
| N-16 | Radix 预分区加速 Shuffle Write | DuckDB | `shuffle/sort_repartitioner.rs` | P2 |

---

*文档版本: v3.0 | 范围: 全项目（含 Native 层深度优化）| 方法: 代码精确分析 + Velox/DuckDB 参考*
