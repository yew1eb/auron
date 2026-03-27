# Apache Auron 全项目优化地图

> **版本**: v2.0
> **日期**: 2026-03-26
> **范围**: 全项目（JVM 扩展层 + JNI 边界 + Native Engine）
> **方法论**: 基于代码精确定位，所有优化点均附文件路径和行号；收益估计注明是否经过 benchmark 验证

---

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
第三部分：分模块优化详解
  3.1 JVM 数据通路（Arrow IO + NativeHelper）
  3.2 JNI 边界（Plan 序列化 + Protobuf）
  3.3 计划转换层（AuronConvertStrategy + Native*Base）
  3.4 内存管理（auron-memmgr）
  3.5 Native 算子——Join
  3.6 Native 算子——聚合（Agg）
  3.7 Native 算子——Shuffle 写
  3.8 Native 算子——排序（Sort）
  3.9 Native 表达式与函数
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

*文档版本: v2.0 | 范围: 全项目 | 方法: 代码精确分析*
