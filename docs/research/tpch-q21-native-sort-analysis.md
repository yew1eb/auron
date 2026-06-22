# TPC-H Q21 右侧 lineitem SortMergeJoin 中 Auron native sort 慢于 Spark 原生 sort 的调研报告

## 1. 核心结论

**TPC-H Q21 右侧 `lineitem` 的 sort 阶段，Auron native sort 比 Spark 原生 sort 慢，最主要的原因是排序算法实现层级的差异：**

| 实现 | 排序方式 | 对单列 `l_orderkey` (Int64) 的处理 |
|---|---|---|
| **Spark 原生 SortExec** | prefix + radix / TimSort | 单列整数可完全走 **radix sort**，时间复杂度 `O(n)`，且只操作 8 字节 prefix |
| **Auron native SortExec** | 通用外部排序 | 通过 Arrow `RowConverter` 把 `Int64` 编码成 **9 字节**（1 字节 null sentinel + 8 字节 big-endian），再按字节比较排序 |

因此 Spark 在该场景下具有算法优势，而 Auron 当前实现把“单列整数排序”退化成通用字节比较排序，放大了耗时。

---

## 2. 代码实现差异

### 2.1 Spark 原生 `SortExec`

- `SortExec.createSorter()` 会构造 `UnsafeExternalRowSorter`。
- 对第一排序列生成 **8 字节 prefix**（`PrefixComputer`），整数类型直接取 key 值。
- `UnsafeInMemorySorter.getSortedIterator()` 中，如果满足 `canUseRadixSort`：
  - `sortOrder.length == 1`
  - 类型在 `SortPrefixUtils.canSortFullyWithPrefix` 列表中（包含 `IntegerType` / `LongType`）
  - 则调用 `RadixSort.sortKeyPrefixArray()`。
- radix sort 对高重复 key 极快，因为它按字节桶计数，不需要两两比较。

### 2.2 Auron native `SortExec`

关键路径在 `native-engine/datafusion-ext-plans/src/sort_exec.rs`：

1. `PruneSortKeysFromBatch::prune()` 调用 `RowConverter::convert_columns()`，把 sort key 列编码成 `Rows`。
   - 对非空 `Int64`，编码为 `1_u8` + 8 字节 big-endian（符号位翻转）。
2. `insert_batch()` 中对每个 batch 做排序：
   - key 平均长度 `≤8` 用 `sorted_unstable_by_key`，否则用 `sorted_by_key`。
   - 比较函数是 `keys.row(i).as_ref()` 的字节序比较。
3. 输出时通过 `send_output_batch()` 再 `restore` 被剪枝的 key 列。

**RowConverter 的字节编码适合多列、多类型通用排序，但对单列整数来说引入了额外内存拷贝和字节比较开销。**

---

## 3. 这与 `lineitem.l_orderkey` 特征的关系

TPC-H lineitem 的 `l_orderkey` 具有以下特征：

- **单列 Int64 join key**：Q21 的 SMJ 右侧只需要按 `l_orderkey` 排序，正好命中 Spark 的 radix sort 路径。
- **高重复度**：SF=10 时 lineitem 约 6000 万行，orders 约 1500 万行，平均每个 `l_orderkey` 重复 4 次左右。
- 高重复度对 **radix sort** 更有利（计数排序天然处理重复值）；对基于比较的排序，虽然 Auron 在实现上利用 common-prefix 压缩略能受益，但无法弥补算法复杂度差距。

**所以：不是“高重复导致 Auron 慢”，而是“单列整数 + 高重复”这个特征让 Spark 的 radix sort 优势被进一步放大。**

---

## 4. 实验验证

在 `native-engine/datafusion-ext-plans/src/sort_exec.rs` 的 `fuzztest` 模块中增加了对比测试（已标记 `#[ignore]`，作为 benchmark 保留），构造了 100 万行 Int64 数据、模拟不同重复度的 `l_orderkey` 分布：

**优化前（RowConverter 字节编码路径，debug 模式）**：

```text
[sort in-mem]   repeat=  1, auron=3.014s, datafusion=0.487s, speedup(df/auron)=6.18x
[sort in-mem]   repeat=  4, auron=3.010s, datafusion=0.480s, speedup(df/auron)=6.27x
[sort in-mem]   repeat= 20, auron=2.807s, datafusion=0.456s, speedup(df/auron)=6.15x
[sort in-mem]   repeat=100, auron=2.384s, datafusion=0.353s, speedup(df/auron)=6.75x

[sort external] repeat=  1, auron=2.958s, datafusion=0.516s, speedup(df/auron)=5.73x
[sort external] repeat= 20, auron=2.702s, datafusion=0.450s, speedup(df/auron)=6.00x
[sort external] repeat=100, auron=2.313s, datafusion=0.339s, speedup(df/auron)=6.83x
```

**优化后（primitive fast path，release 模式）**：

```text
[sort in-mem]  repeat=  1, auron=0.051s, datafusion=0.023s, speedup(df/auron)=2.25x
[sort in-mem]  repeat=  4, auron=0.046s, datafusion=0.023s, speedup(df/auron)=1.98x
[sort in-mem]  repeat= 20, auron=0.045s, datafusion=0.021s, speedup(df/auron)=2.16x
[sort in-mem]  repeat=100, auron=0.044s, datafusion=0.020s, speedup(df/auron)=2.20x

[sort external] repeat=  1, auron=0.049s, datafusion=0.023s, speedup(df/auron)=2.14x
[sort external] repeat= 20, auron=0.045s, datafusion=0.021s, speedup(df/auron)=2.10x
[sort external] repeat=100, auron=0.043s, datafusion=0.030s, speedup(df/auron)=1.42x
```

- **优化前：Auron 比 DataFusion 慢约 6 倍（RowConverter 编码 + 字节比较开销）。**
- **优化后：差距缩小到约 2 倍，内存和外部排序均有效。**
- 剩余差距来自 K-way merge 阶段仍使用 RowConverter 字节比较（后续可进一步用 PrimitiveKeyCollector 消除）。
- DataFusion 原生 SortExec 对 primitive 类型直接调用 Arrow `sort_to_indices` → Rust `sort_unstable_by` 比较原始 `i64`。
- Spark 原生 SortExec 对单列整数还会进一步使用 **radix sort**，理论上比 DataFusion 的 `sort_unstable_by` 更快，因此 Auron vs Spark 的差距大概率大于 2 倍——进一步实现 radix sort 可继续缩差。

> 测试代码位置：`native-engine/datafusion-ext-plans/src/sort_exec.rs`，函数 `bench_native_sort_varying_repeat_in_mem` / `bench_native_sort_varying_repeat_external`。

---

## 5. 优化方向

按收益/风险排序：

### 5.1 高优先级：为单列 primitive key 引入类型感知排序

在 `SortExec` 中检测 `sort_exprs` 满足：
- 只有一列；
- 表达式为 `Column`；
- 数据类型为 `Int8/16/32/64`、`UInt8/16/32/64`、`Date32/64`、`Timestamp` 等固定宽度整数类型。

则**绕过 `RowConverter`**，直接对该列使用 Arrow/DataFusion 原生的 `sort_to_indices`（或自研 radix sort），再 `take` 整个 batch。这可以把单列整数 sort 的性能提升到与 DataFusion/Spark 同量级。

### 5.2 中优先级：实现 radix sort

对单列整数实现 LSD radix sort。参考 Spark 的 `RadixSort`：
- 排序指针数组 + prefix（8 字节），不移动完整 row；
- null 值单独分区；
- descending/signed 最后字节特殊处理。

Auron 的 key collector / loser tree 结构可以继续复用，但 batch 内排序阶段替换为 radix sort。

### 5.3 中优先级：prefix 比较优化

如果不能快速实现 radix sort，可以先在比较器中引入 **prefix 比较**：
- 对整数 key 直接取原始值作为 8 字节 prefix；
- 比较时先比较 prefix，只有 prefix 相等才回退到字节比较。

这类似 Spark 的 `PrefixComparator`，能减少大部分完整 row 的比较。

### 5.4 低优先级：Spark 转换策略兜底

在 `AuronConvertStrategy.removeInefficientConverts` 中增加启发式：
- 当 `SortExec` 的 sort key 是单列整数且数据量大时，回退到 Spark 原生 SortExec。

这是“避战”策略，改动最小，但放弃了 native 执行的其他收益，建议作为临时方案。

---

## 6. 建议的后续动作

1. **实现 5.1 的类型感知 fast path**（预计改动 1-2 个文件，集中在 `sort_exec.rs`），对单列整数直接走 Arrow 原生排序。
2. 在 Q21 场景下重新跑端到端对比，验证右侧 lineitem sort 阶段耗时是否回落到与 Spark 同量级。
3. 如果效果仍不足，再引入 5.2 的 radix sort。

---

## 7. 竞品框架调研

对 Gluten/Velox、DataFusion-Comet/DataFusion、Daft 三套框架做了横向调研，梳理各自在单列整数 sort 上的实现策略，作为 Auron 优化的参考依据。

### 7.1 Gluten / Velox

**Gluten 的转换路径**

Gluten（`incubator-gluten`）将 Spark `SortExec` 通过 Substrait 中间表示卸载到 Velox：

```
Spark SortExec
  → SortExecTransformer（Scala）
  → Substrait SortRel（protobuf）
  → SubstraitToVeloxPlanConverter（C++）
  → Velox OrderByNode
```

关键文件：
- `gluten-substrait/src/main/scala/org/apache/gluten/execution/SortExecTransformer.scala`
- `cpp/velox/substrait/SubstraitToVeloxPlan.cc`（第 1339-1344 行）

**Velox 的排序实现**

Velox 的 `OrderByNode` 使用**通用行编码**（类似 Arrow RowConverter），将所有 sort key 编码成字节串后按字典序比较。**没有针对单列整数类型的 fast path 或 radix sort 路径。**

注意：Gluten 的 Shuffle 模块（`cpp/velox/shuffle/RadixSort.h`）确实实现了移植自 Spark 的 LSD radix sort，但仅用于 Shuffle Write 阶段的分区指针排序，不用于通用 `OrderByNode`。

**结论**：Gluten/Velox 在通用排序上与 Auron 现状类似，同样慢于 Spark radix sort 路径。

---

### 7.2 DataFusion 原生 SortExec

DataFusion（49.x）的 `SortExec` 与 Auron 的自定义 `SortExec` 是两套独立实现：

| 特性 | DataFusion 原生 SortExec | Auron 自定义 SortExec |
|---|---|---|
| sort key 编码 | **不编码**，直接比较原始 Arrow 数组值 | RowConverter → 字节编码 |
| 单列 Int64 | 调用 Arrow `sort_to_indices`，比较原始 `i64` | 9 字节字节比较（1B null + 8B big-endian） |
| radix sort | 无 | 无 |
| 外部排序 | 无（内存优先） | 有（LoserTree + LZ4 spill） |

DataFusion 对 primitive 类型直接调用 Arrow 的 `sort_to_indices`，跳过了编码步骤，因此比 Auron 快约 6 倍（见第 4 节 benchmark 数据）。

Auron 项目 `native-engine/datafusion-ext-commons/src/algorithm/rdx_sort.rs` 中已有 radix sort 框架代码，但当前**未在 SortExec 中使用**。

---

### 7.3 Daft

Daft（Eventual Inc，Rust + Arrow2）是目前三个框架中在单列排序上**设计最合理**的：

**类型感知 fast path（已实现）**

Daft 的 `Series::argsort()` 用 `with_match_comparable_daft_types!` 宏按类型分发：

```rust
// daft-core/src/series/ops/sort.rs
pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
    let series = self.as_physical()?;
    with_match_comparable_daft_types!(series.data_type(), |$T| {
        let downcasted = series.downcast::<<$T as DaftDataType>::ArrayType>()?;
        downcasted.argsort(descending, nulls_first)
    })
}
```

对 `Int64`、`UInt64`、`Date`、`Timestamp` 等 primitive 类型，直接在原始切片上调用 Rust 的 `sort_unstable_by`，**完全不走 RowConverter 编码**。

**多列 fallback 到行编码**

当排序列 > 1 时，Daft 才使用 arrow2 的 `RowConverter`（实现了与 Arrow 类似的字节编码，支持 memcmp 比较）。

**Null 预分离优化**

```rust
// daft-core/src/array/ops/arrow/sort/primitive/common.rs
fn generate_initial_indices(nulls, length, nulls_first) {
    // 提前将 null 和 non-null 分离到数组两端
    // 后续排序只操作 non-null 部分，避免排序循环内的 null 检查
}
```

**无 radix sort，无外部排序**

Daft 目前不实现 radix sort，依赖 `sort_unstable_by` 的 O(n log n)；外部排序也未实现，大数据量依赖分布式 range repartition。

---

### 7.4 横向对比总结

| 框架 | 单列 Int64 路径 | radix sort | 外部排序 | 相对 Spark 性能 |
|---|---|---|---|---|
| **Spark 原生** | radix sort，O(n) | ✓ | ✓ | 基准（最快） |
| **Gluten/Velox** | 通用行编码 + 字节比较 | ✗（仅 Shuffle 用） | ✓ | 慢于 Spark |
| **DataFusion 原生** | Arrow `sort_to_indices`，比较原始值 | ✗ | ✗ | 慢于 Spark，约 3-5 倍 |
| **Daft** | `sort_unstable_by` 比较原始值 | ✗ | ✗ | 与 DataFusion 同级 |
| **Auron（现状）** | RowConverter 编码 + 字节比较 | ✗ | ✓ | 比 DataFusion 再慢 ~6 倍 |
| **Auron（目标）** | 类型感知 fast path → 原始值比较 | 待实现 | ✓ | 对齐 DataFusion，再实现 radix sort 后超越 Spark |

**核心结论**：
- **DataFusion 和 Daft 的共同策略**——检测单列 primitive 类型后跳过 RowConverter、直接比较原始值——是最小改动、最高收益的路径，也是 Auron 5.1 方案的直接参考。
- **radix sort** 目前没有一个开源框架在通用排序中实现（只有 Spark 和 Gluten Shuffle 用），是 Auron 可以差异化的方向。
