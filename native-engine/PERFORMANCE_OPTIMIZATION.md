# Native Engine 性能优化分析

本文档记录了在 Auron native engine Rust 代码库中识别出的性能优化机会，涵盖哈希表、Shuffle、聚合、排序和 Join 算子等模块。

---

## 汇总表

| 优先级 | 问题描述 | 所在文件 |
|--------|----------|----------|
| 高 | `RangePartitioning` 每个批次都重建 `RowConverter` | `datafusion-ext-plans/src/shuffle/mod.rs` |
| 高 | `radix_sort_by_key` 外层循环中 `retain` 调用的时间复杂度为 O(k²) | `datafusion-ext-commons/src/algorithm/rdx_sort.rs` |
| 高 | `GetMapValueExpr` Map 查找使用线性搜索 | `datafusion-ext-exprs/src/get_map_value.rs` |
| 高 | `CachedExprsEvaluator` 双重加锁问题 | `datafusion-ext-plans/src/common/cached_exprs_evaluator.rs` |
| 中 | `AggHashMap::rehash` 仅部分利用了 SIMD | `datafusion-ext-plans/src/agg/agg_hash_map.rs` |
| 中 | `JoinHashMap::lookup_many` 每次探测步骤执行两次 SIMD 比较 | `datafusion-ext-plans/src/joins/join_hash_map.rs` |
| 中 | Shuffle 暂存 `Vec` 在每次 `flush_staging` 时都重新分配 | `datafusion-ext-plans/src/shuffle/buffered_data.rs` |
| 中 | BHJ 探测阶段：null key 过滤导致额外 `collect` 和双游标模式 | `datafusion-ext-plans/src/joins/bhj/full_join.rs` |
| 中 | `MergingData::entries` 存储 16 字节的 4 元组，可压缩至 12 字节 | `datafusion-ext-plans/src/agg/agg_table.rs` |
| 中 | `CachedExprsEvaluator::Cache` 每次表达式求值加锁两次 | `datafusion-ext-plans/src/common/cached_exprs_evaluator.rs` |
| 中 | SMJ `FullJoiner::flush` 每次 flush 都重建 `BatchInterleaver` | `datafusion-ext-plans/src/joins/smj/full_join.rs` |
| 中 | `AccPrimColumn::update_value` 在聚合热循环中每行做一次 `BitVec` 随机访问 | `datafusion-ext-plans/src/agg/acc.rs` |
| 中 | `AggContext::create_grouping_rows` RowConverter 加锁开销 | `datafusion-ext-plans/src/agg/agg_ctx.rs` |
| 中 | `SortExec` 缺乏类型特化排序路径 | `datafusion-ext-plans/src/sort_exec.rs` |
| 中 | 字符串函数（`string_lower`/`upper`）逐行处理 | `datafusion-ext-functions/src/spark_strings.rs` |
| 低 | `hash_one` 对 `List`/`Map`/`Struct` 列没有批量处理路径 | `datafusion-ext-commons/src/spark_hash.rs` |
| 低 | `TryCastExpr` 标量值转换分配临时数组 | `datafusion-ext-exprs/src/cast.rs` |
| 低 | `spark_get_json_object` 逐行解析 JSON | `datafusion-ext-functions/src/spark_get_json_object.rs` |
| 低 | 日期函数重复类型转换 | `datafusion-ext-functions/src/spark_dates.rs` |

---

## 1. `RangePartitioning` 每批次重建 `RowConverter`（高）

**文件：** `datafusion-ext-plans/src/shuffle/mod.rs:210`

```rust
fn evaluate_range_partition_ids(
    batch: &RecordBatch,
    sort_expr: &Vec<PhysicalSortExpr>,
    bound_rows: &Arc<Rows>,
) -> Result<Vec<u32>> {
    // ← 每次调用都重新构建
    let sort_row_converter = Arc::new(SyncMutex::new(RowConverter::new(
        sort_expr.iter()
            .map(|expr| Ok(SortField::new_with_options(...)))
            .collect::<Result<Vec<SortField>>>()?,
    )?));
    ...
}
```

**问题：** `RowConverter::new` 会分配内部状态（字段编码器、缓冲区池）。该函数在每个进来的 `RecordBatch` 上都被调用，因此每次都触发分配，共计 O(批次数) 次，而实际上 Schema 从未改变。

**建议修复：** 将 `RowConverter` 缓存为 `Partitioning::RangePartitioning` 的额外字段（如 `Arc<Mutex<RowConverter>>`），仅在构造时初始化一次，跨批次复用。

---

## 2. `radix_sort_by_key` 外层循环 O(k²) 的 `retain` 调用（高）

**文件：** `datafusion-ext-commons/src/algorithm/rdx_sort.rs:56`

```rust
while {
    inexhausted_part_indices.retain(|&i| parts[i].cur < parts[i].end);
    inexhausted_part_indices.len() > 1
} {
    for &part_idx in inexhausted_part_indices.iter() {
        for item_idx in cur..end { ... }
    }
}
```

**问题：** 每轮外层迭代，`retain` 都会扫描完整的 `inexhausted_part_indices` 切片。在分区数较大时（如 1000 路 Shuffle），最坏情况下复杂度为 O(k²)。每轮还会对一个从 `k` 开始逐步缩小的 Vec 做多次小范围扫描。

**建议修复：** 用 swap-remove 惯用法替换 `retain`，使已耗尽的分区以 O(1) 跳过，而非每轮 O(k)：

```rust
let mut active = inexhausted_part_indices.len();
while active > 1 {
    let mut i = 0;
    while i < active {
        if parts[inexhausted_part_indices[i]].cur >= parts[inexhausted_part_indices[i]].end {
            inexhausted_part_indices.swap(i, active - 1);
            active -= 1;
        } else {
            i += 1;
        }
    }
    // 处理活跃分区 ...
}
```

---

## 3. `GetMapValueExpr` Map 查找使用线性搜索（高）

**文件：** `datafusion-ext-exprs/src/get_map_value.rs:100-117`

```rust
for (start, end) in as_map_array.value_offsets().iter().map(...) {
    let mut found = false;
    for key_idx in start..end {        // ← 内层循环：线性搜索 O(n*m)
        if comparator(key_idx, 0).is_eq() {
            found = true;
            mutable.extend(0, key_idx, key_idx + 1);
            break;
        }
    }
    if !found {
        mutable.extend_nulls(1);
    }
}
```

**问题：** 对每个 Map 条目进行线性搜索，时间复杂度 O(n*m)。`MutableArrayData::extend` 每次调用都有边界检查开销，且未利用 SIMD 加速键比较。对于包含大 Map 的数据，这可能成为显著瓶颈。

**建议修复：**
1. **小 Map 优化**：如果键数量少（< 8），保持线性搜索并展开循环
2. **大 Map 优化**：构建临时哈希表或使用二分查找（如果键有序）
3. **SIMD 批量比较**：对固定宽度类型（如 int）使用 SIMD 并行比较多个键

```rust
// 优化示例：对于常见的小 Map（键数<8），展开循环
match end - start {
    0 => mutable.extend_nulls(1),
    1 => { /* 直接比较，无循环开销 */ },
    2 => { /* 展开两次比较 */ },
    // ...
    _ => { /* 回退到通用循环或哈希查找 */ }
}
```

---

## 4. `CachedExprsEvaluator` 双重加锁优化（高）

**文件：** `datafusion-ext-plans/src/common/cached_exprs_evaluator.rs:423-448`

```rust
fn get(&self, id: usize, evaluate_on_vacant: impl Fn() -> Result<ColumnarValue>) -> Result<ColumnarValue> {
    if let Some(cached) = &self.values.lock()[id] {  // ← 加锁 #1
        return Ok(cached.clone());
    }
    let cached = evaluate_on_vacant()?;
    self.values.lock()[id] = Some(cached.clone());   // ← 加锁 #2
    Ok(cached)
}

fn update_all(&self, on_update: ...) -> Result<()> {
    let current_values = self.values.lock().clone();  // ← 克隆完整 Vec
    let updated_values = current_values.into_iter()...collect()?;
    *self.values.lock() = updated_values;             // ← 第二次加锁
    Ok(())
}
```

**问题：** `get` 方法对 `parking_lot::Mutex` 加锁两次（读取检查一次、写入一次）。`update_all` 将缓存 Vec 克隆到临时变量，对克隆应用 `on_update`，再替换回去。当缓存的子表达式较多、过滤谓词较多时，这会使锁竞争加倍，且每个谓词应用时都会触发一次分配。

**建议修复：**
- `get` 中：用单次 `lock()` 调用完成"检查后写入"的全过程，避免双重加锁
- `update_all` 中：在单次加锁范围内对 `Vec` 原地 `iter_mut`，避免克隆和二次替换

```rust
fn get(&self, id: usize, eval: impl Fn() -> Result<ColumnarValue>) -> Result<ColumnarValue> {
    let mut guard = self.values.lock();
    if let Some(cached) = &guard[id] {
        return Ok(cached.clone());
    }
    drop(guard); // 求值期间（可能开销较大）先释放锁
    let result = eval()?;
    self.values.lock()[id] = Some(result.clone());
    Ok(result)
}
```

---

## 5. `AggHashMap::rehash` 仅部分利用 SIMD（中）

**文件：** `datafusion-ext-plans/src/agg/agg_hash_map.rs:139`

```rust
fn rehash(&mut self, map_mod_bits: u32) {
    let mut rehashed_map = unchecked!(vec![MapValueGroup::default(); 1 << map_mod_bits]);
    for group in self.map.drain(..) {
        let new_entries = group.hashes % new_mods;   // SIMD 一次处理 8 个哈希取模
        for &e in new_entries.as_array().iter().rev() {
            prefetch_write_data!(&rehashed_map[e as usize]);  // 预取
        }
        // ← 插入仍是逐元素的标量开放寻址
        for j in 0..MAP_VALUE_GROUP_SIZE {
            if non_empty.test(j) { ... loop { ... } }
        }
    }
}
```

**问题：** SIMD 仅用于一次计算 8 个目标槽位索引并预取，但实际将元素插回 `rehashed_map` 的过程仍是逐元素的标量开放寻址循环。哈希冲突链可能导致每个元素多次 cache miss。

**建议修复：** 在 rehash 时，先将所有 `(hash, value)` 对收集到一个扁平 `Vec` 中，再按 `new_entries` 做一次基数排序，然后按顺序批量填充各 group。这样随机写变为顺序写，已有的预取也能得到更充分利用。

---

## 6. `JoinHashMap::lookup_many` 每次探测执行两次 SIMD 比较（中）

**文件：** `datafusion-ext-plans/src/joins/join_hash_map.rs:255`

```rust
loop {
    let hash_matched = self.map[e].hashes.simd_eq(Simd::splat(hashes[i]));
    let empty = self.map[e].hashes.simd_eq(Simd::splat(0));
    // 两次 simd_eq + 一次 OR
    if let Some(pos) = (hash_matched | empty).first_set() { ... }
    e += 1;
}
```

**问题：** 每次探测步骤无条件执行两次 SIMD 比较再做 OR。在最常见的无哈希碰撞场景下（第一次探测即命中空槽），`hash_matched` 的比较是多余的。

**建议修复：** 参照 `agg_hash_map.rs::upsert_one_impl` 的模式，先检查 `hash_matched`，命中时直接 break；仅当该 group 中没有匹配项时，才降级执行 `simd_eq(0)`。在常见路径（空槽或直接命中）上可将 SIMD 开销减半：

```rust
loop {
    let hash_matched = self.map[e].hashes.simd_eq(Simd::splat(hashes[i]));
    if let Some(pos) = hash_matched.first_set() {
        // 命中
        hashes[i] = transmute(self.map[e].values[pos]);
        break;
    }
    let empty = self.map[e].hashes.simd_eq(Simd::splat(0));
    if let Some(_) = empty.first_set() {
        // 未命中（到达空槽）
        hashes[i] = transmute(MapValue::EMPTY);
        break;
    }
    e += 1;
    e %= 1 << self.map_mod_bits;
}
```

---

## 7. Shuffle 暂存 `Vec` 每次 `flush_staging` 都重新分配（中）

**文件：** `datafusion-ext-plans/src/shuffle/buffered_data.rs:285`

```rust
fn sort_batches_by_partition_id(...) -> Result<(Vec<u32>, RecordBatch)> {
    // ← 每次 flush_staging 都分配
    let mut partition_indices = batches.iter().enumerate()
        .flat_map(|(batch_idx, batch)| { ... })
        .collect::<Vec<_>>();

    let mut part_counts = vec![0; num_partitions];
    radix_sort_by_key(&mut partition_indices, &mut part_counts, ...);
    ...
}
```

**问题：** `partition_indices`（`Vec<(u32, u32, u32)>`）和 `part_counts`（`Vec<usize>`）在每次 `flush_staging` 调用时都被分配、排序、释放。对于有大量小批次的工作负载，这在热路径上造成了较高的内存分配器压力。

**建议修复：** 在 `BufferedData` 中添加两个可复用的暂存缓冲区（`staging_scratch_indices: Vec<(u32,u32,u32)>` 和 `part_counts_scratch: Vec<usize>`），在每次 `flush_staging` 开始时 clear 并以 `&mut` 引用传入，从而将分配开销摊销到 `BufferedData` 的整个生命周期。

---

## 8. BHJ 探测阶段：null key 过滤导致额外 `collect` 和双游标（中）

**文件：** `datafusion-ext-plans/src/joins/bhj/full_join.rs:243`

```rust
let map_values = probed_side_search_time.with_timer(|| {
    let probed_hashes = if let Some(probed_valids) = &probed_valids {
        probed_hashes.iter().enumerate()
            .filter_map(|(row_idx, &hash)| probed_valids.is_valid(row_idx).then_some(hash))
            .collect()   // ← 分配一个长度更短的 Vec
    } else {
        probed_hashes
    };
    map.lookup_many(probed_hashes)  // 存在 null 时返回长度 < num_rows 的 Vec
});

// 之后：通过单独的计数器仅对有效行推进 hashes_idx
let mut hashes_idx = 0;
for row_idx in 0..probed_batch.num_rows() {
    if valid(row_idx) {
        let map_value = map_values[hashes_idx];
        hashes_idx += 1;
        ...
    }
}
```

**问题：** null 过滤的 collect 产生了中间 Vec，并在热探测循环中引入了双游标（`row_idx` / `hashes_idx`）模式，阻止了编译器对循环体进行自动向量化。

**建议修复：** 始终将完整长度的哈希数组传给 `lookup_many`。null key 行使用哈希值 `0`（映射到 `MapValue::EMPTY`，因为哈希表保证存储的哈希均非零）。这样探测循环变为简单的单游标 `for row_idx in 0..N`，遇到 null 时 early-continue，更易被优化：

```rust
// lookup 之前：将 null 位置置零，而非过滤掉
if let Some(pv) = &probed_valids {
    for (i, h) in probed_hashes.iter_mut().enumerate() {
        if pv.is_null(i) { *h = 0; }
    }
}
let map_values = map.lookup_many(probed_hashes); // 长度始终等于 num_rows
```

---

## 9. `MergingData::entries` 4 元组过大（中）

**文件：** `datafusion-ext-plans/src/agg/agg_table.rs:596`

```rust
entries: Vec<(u32, u32, u32, u32)>, // (bucket_id, batch_idx, row_idx, acc_idx)
```

**问题：** 每个 entry 占 16 字节。`bucket_id` 的上限为 `num_spill_buckets`，其返回类型（`agg_table.rs:838`）为 `u16`，因此第一个字段浪费了 2 字节。对于数百万行，这会造成数十 MB 的不必要内存占用。基数排序在交换元素时也要搬移完整的 16 字节。

**建议修复：** 将类型改为 `(u16, u16, u32, u32)` = 12 字节，在插入时将 `bucket_id` 转换为 `u16`。`swap_unchecked` 对 12 字节值的操作触碰更少的 cache line，也能改善基数排序性能。

---

## 10. `CachedExprsEvaluator::Cache` 每次表达式求值加锁两次（中）

**文件：** `datafusion-ext-plans/src/common/cached_exprs_evaluator.rs:423`

```rust
fn get(
    &self,
    id: usize,
    evaluate_on_vacant: impl Fn() -> Result<ColumnarValue>,
) -> Result<ColumnarValue> {
    if let Some(cached) = &self.values.lock()[id] {  // ← 加锁 #1：读取检查
        return Ok(cached.clone());
    }
    let cached = evaluate_on_vacant()?;
    self.values.lock()[id] = Some(cached.clone());   // ← 加锁 #2：写入
    Ok(cached)
}
```

`update_all` 还会克隆整个缓存 Vec：

```rust
fn update_all(&self, on_update: ...) -> Result<()> {
    let current_values = self.values.lock().clone();  // ← 克隆完整 Vec
    let updated_values = current_values.into_iter()...collect()?;
    *self.values.lock() = updated_values;             // ← 第二次加锁
    Ok(())
}
```

**问题：** `get` 对 `parking_lot::Mutex` 加锁两次（读取检查一次、写入一次）。`update_all` 将缓存 Vec 克隆到临时变量，对克隆应用 `on_update`，再替换回去。当缓存的子表达式较多、过滤谓词较多时，这会使锁竞争加倍，且每个谓词应用时都会触发一次分配。

**建议修复：**
- `get` 中：用单次 `lock()` 调用完成"检查后写入"的全过程，避免双重加锁。
- `update_all` 中：在单次加锁范围内对 `Vec` 原地 `iter_mut`，避免克隆和二次替换。

```rust
fn get(&self, id: usize, eval: impl Fn() -> Result<ColumnarValue>) -> Result<ColumnarValue> {
    let mut guard = self.values.lock();
    if let Some(cached) = &guard[id] {
        return Ok(cached.clone());
    }
    drop(guard); // 求值期间（可能开销较大）先释放锁
    let result = eval()?;
    self.values.lock()[id] = Some(result.clone());
    Ok(result)
}
```

---

## 11. SMJ `FullJoiner::flush` 每次 flush 重建 `BatchInterleaver`（中）

**文件：** `datafusion-ext-plans/src/joins/smj/full_join.rs:63`

```rust
async fn flush(...) -> Result<()> {
    // ← 每次 flush 都重新构建
    let lbatch_interleaver = create_batch_interleaver(cur1.batches(), false)?;
    let rbatch_interleaver = create_batch_interleaver(cur2.batches(), false)?;
    let lcols = lbatch_interleaver(&lindices)?;
    let rcols = rbatch_interleaver(&rindices)?;
    ...
}
```

**问题：** `create_batch_interleaver` 会检查批次 Schema、预计算每列的交织策略并返回闭包。在 SMJ 中，`flush` 每输出一个批次就被调用一次（通常每 `batch_size = 8192` 行一次），因此这部分构建开销被支付了 O(输出批次数) 次。对于等值键组较多、Join 扇出较高的查询，这一开销可量化。

**建议修复：** 将交织器闭包缓存在 `FullJoiner` 中，仅当 `cur1.batches()` 或 `cur2.batches()` 发生变化时（例如加载新的 spill 页）才重建：

```rust
pub struct FullJoiner<const L_OUTER: bool, const R_OUTER: bool> {
    ...
    linterleaver: Option<BatchInterleaver>,
    rinterleaver: Option<BatchInterleaver>,
    lbatches_len: usize,  // 重建触发条件
    rbatches_len: usize,
}
```

本仓库中的 `batch_interleaver` benchmark 直接测量了构建开销与调用开销的比例，可用于量化该优化的收益。

---

## 12. `AccPrimColumn::update_value` 聚合热循环中每行 `BitVec` 随机访问（中）

**文件：** `datafusion-ext-plans/src/agg/acc.rs:265`

```rust
pub fn update_value(&mut self, idx: usize, default_value: T, update: impl Fn(T) -> T) {
    if self.valids[idx] {           // ← BitVec 随机访问：读取一个 word，移位，掩码
        self.values[idx] = update(self.values[idx]);
    } else {
        self.values[idx] = default_value;
        self.valids.set(idx, true); // ← 另一次 BitVec 随机访问
    }
}
```

**问题：** 在聚合热循环中（`sum.rs::partial_update`、`maxmin.rs::partial_update`、`avg.rs::partial_update`），`idx_for_zipped!` 宏对每行调用一次 `update_value`，每次都触发一次 `BitVec` 随机读取（读一个 word + 提取位）。对于 `IdxSelection::Range` 范围内的列式聚合，这是多余的：整段范围可以用紧密循环处理，`valids` 在结束后批量置位，`values` 可利用 SIMD 累加。

**建议修复：** 为 `AccPrimColumn` 添加 `bulk_update_range` 方法，接受 `IdxSelection::Range(begin, end)` 并以标量或 SIMD 循环处理，最后用 `BitVec::fill` 或 `set_all` 批量写入 valid 位。现有的 `update_value` 保留用于非 Range 场景（merge、spill）：

```rust
// SUM/AVG/MIN/MAX 在连续 Range 上的快速路径
pub fn update_range(&mut self, begin: usize, end: usize, src: &[T]) {
    // values[begin..end] += src[begin..end]（或 min/max 比较）
    // valids[begin..end] = true（批量置位）
}
```

---

## 13. `AggContext::create_grouping_rows` RowConverter 加锁开销（中）

**文件：** `datafusion-ext-plans/src/agg/agg_ctx.rs:233-244`

```rust
pub fn create_grouping_rows(&self, input_batch: &RecordBatch) -> Result<Rows> {
    let grouping_arrays: Vec<ArrayRef> = self
        .groupings
        .iter()
        .map(|grouping| grouping.expr.evaluate(&input_batch))
        .map(|r| r.and_then(|columnar| columnar.into_array(input_batch.num_rows())))
        .collect::<Result<_>>()?;
    Ok(self
        .grouping_row_converter
        .lock()  // ← 每次都有锁开销
        .convert_columns(&grouping_arrays)?)
}
```

**问题：** 每次创建 grouping rows 都需要获取 `Mutex` 锁，即使 `RowConverter` 的内部状态是只读的。在高并发聚合场景下，这把锁可能成为瓶颈。

**建议修复：**
1. 使用线程本地存储（Thread Local Storage）为每个线程缓存一个 `RowConverter` 实例
2. 或者使用 `RwLock`，读取时并发，仅在初始化时写入

```rust
thread_local! {
    static ROW_CONVERTER_CACHE: RefCell<HashMap<SchemaRef, RowConverter>> = ...;
}

pub fn create_grouping_rows(&self, input_batch: &RecordBatch) -> Result<Rows> {
    // 在线程本地缓存中获取或创建 converter
    ROW_CONVERTER_CACHE.with(|cache| {
        // ...
    })
}
```

---

## 14. `SortExec` 缺乏类型特化排序路径（中）

**文件：** `datafusion-ext-plans/src/sort_exec.rs`

**问题：** 当前实现使用 `RowConverter` 进行键转换，存在序列化开销。对于简单类型（int, float），可以使用直接比较而非行格式。此外，对于整数类型可以使用基数排序替代比较排序。

**建议修复：**
1. **类型特化**：对常见类型（Int32, Int64, Float64, String）实现专门的排序路径
2. **SIMD 比较**：对固定宽度类型使用 SIMD 批量比较
3. **基数排序**：对整数类型使用基数排序替代比较排序

```rust
// 特化路径示例
match key_type {
    DataType::Int32 => simd_sort_i32(keys, values),
    DataType::Int64 => simd_sort_i64(keys, values),
    _ => generic_sort(keys, values),  // 回退到通用路径
}
```

---

## 15. 字符串函数逐行处理（中）

**文件：** `datafusion-ext-functions/src/spark_strings.rs:31-46`

```rust
pub fn string_lower(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
            as_string_array(array)?
                .into_iter()
                .map(|s| s.map(|s| s.to_lowercase())),  // ← 逐行处理
        ))))),
        // ...
    }
}
```

**问题：** 字符串函数（`string_lower`、`string_upper`、`string_repeat` 等）逐行迭代处理，无法利用 SIMD 或并行处理。对于大数据集，这可能成为 CPU 瓶颈。

**建议修复：**
1. **SIMD 字符串处理**：对 ASCII 字符使用 SIMD 批量转换大小写
2. **并行处理**：对大数据量使用 `rayon` 并行迭代
3. **特殊路径**：检测全 ASCII 字符串，使用快速路径

```rust
// ASCII 快速路径
if s.is_ascii() {
    // 使用 SIMD 批量转换
    s.as_bytes()
     .chunks_exact(32)
     .map(|chunk| chunk.to_ascii_lowercase_simd())
} else {
    s.to_lowercase()  // Unicode 慢路径
}
```

---

## 16. `hash_one` 对 `List`/`Map`/`Struct` 列缺少批量处理路径（低）

**文件：** `datafusion-ext-commons/src/spark_hash.rs:218`

```rust
_ => {
    for idx in 0..array.len() {
        hash_one(array, idx, &mut hashes_buffer[idx], h);
    }
}
```

**问题：** 对于 `List`、`Map`、`Struct` 等复杂列类型，`hash_one` 逐行调用并递归下降。这既无法向量化，又在每个元素上触发虚函数分发（`as_any().downcast_ref`）。

**建议修复：**
- 对 `List<T>`（`T` 为基本类型）：展平 values buffer，对每行的连续子切片调用一次 `spark_compatible_murmur3_hash`，再通过 offset 算术将结果折叠回行哈希。
- 对 `Struct`：以列为单位迭代字段，每个字段在一次扫描中更新所有行的哈希，与现有 `hash_array` 的列式循环保持一致。

此改动较为复杂，同时可能对上游 DataFusion 项目产生价值。

---

## 17. `TryCastExpr` 标量值转换分配临时数组（低）

**文件：** `datafusion-ext-exprs/src/cast.rs:69-82`

```rust
fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
    Ok(match self.expr.evaluate(batch)? {
        ColumnarValue::Array(array) => ColumnarValue::Array(
            datafusion_ext_commons::arrow::cast::cast(&array, &self.cast_type)?,
        ),
        ColumnarValue::Scalar(scalar) => {
            let array = scalar.to_array()?;  // ← 分配临时数组
            ColumnarValue::Scalar(ScalarValue::try_from_array(
                &datafusion_ext_commons::arrow::cast::cast(&array, &self.cast_type)?,
                0,
            )?)
        }
    })
}
```

**问题：** 标量值转换时先转换为单元素数组，转换后再转回标量，造成不必要的内存分配。

**建议修复：**
1. 添加类型检查快速路径：如果类型相同，直接返回原值
2. 实现标量到标量的直接转换，避免数组分配

```rust
fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
    match self.expr.evaluate(batch)? {
        ColumnarValue::Array(array) => {
            if array.data_type() == &self.cast_type {
                return Ok(ColumnarValue::Array(array)); // 无操作转换
            }
            ColumnarValue::Array(cast(&array, &self.cast_type)?)
        }
        ColumnarValue::Scalar(scalar) => {
            if scalar.data_type() == self.cast_type {
                return Ok(ColumnarValue::Scalar(scalar));
            }
            // 直接转换标量，避免数组分配
            ColumnarValue::Scalar(scalar_cast(&scalar, &self.cast_type)?)
        }
    }
}
```

---

## 18. `spark_get_json_object` 逐行解析 JSON（低）

**文件：** `datafusion-ext-functions/src/spark_get_json_object.rs:69-79`

```rust
let output = json_strings
    .iter()
    .map(|json_string| {
        json_string.and_then(|s| match evaluator.evaluate(s) {
            Ok(Some(matched)) => Some(matched),
            _ => None,
        })
    })
    .collect::<StringArray>();
```

**问题：** 逐行迭代，每行都独立解析 JSON 和执行路径匹配。无法利用批量解析优化，且 `HiveGetJsonObjectEvaluator` 对每行都执行路径匹配。

**建议修复：**
1. **批量 JSON 解析**：使用 `sonic_rs` 的批量解析 API
2. **路径缓存**：缓存解析路径，避免重复计算
3. **SIMD 字符串搜索**：对简单路径（如 `$.field`）使用 SIMD 加速

---

## 19. 日期函数重复类型转换（低）

**文件：** `datafusion-ext-functions/src/spark_dates.rs:34-47`

```rust
pub fn spark_year(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Year)?))
}

pub fn spark_month(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Month)?))
}
```

**问题：** 每个日期函数都重复进行类型转换（`cast`）。如果同一列被多个日期函数使用，转换会被重复执行。

**建议修复：**
1. **批量转换**：在更高层统一转换类型，避免每个函数都转换
2. **缓存转换结果**：如果同一列被多次使用，缓存转换后的数组
3. **计划阶段优化**：在查询计划阶段合并同一列的多个日期提取操作，一次转换后提取多个部分

```rust
// 优化示例：一次提取多个日期部分
fn extract_date_parts(input: &ArrayRef, parts: &[DatePart]) -> Result<Vec<ArrayRef>> {
    let converted = cast(input, &DataType::Date32)?;
    parts.iter()
        .map(|part| date_part(&converted, *part))
        .collect()
}
```

---

## 附录：优化优先级与预期收益

### 高优先级（立即实施）

| 优化项 | 预期收益 | 实施难度 | 相关模块 |
|-------|---------|---------|---------|
| 修复 CachedExprsEvaluator 双重加锁 | 10-30% 过滤性能提升 | 低 | Filter/Project |
| JoinHashMap::lookup_many SIMD 优化 | 20-50% Join 性能提升 | 中 | Join |
| GetMapValueExpr 线性搜索优化 | 2-5x Map 查找性能 | 中 | Expression |
| AggHashMap rehash 批量插入 | 15-30% 聚合性能提升 | 中 | Aggregation |
| RangePartitioning RowConverter 缓存 | 10-20% Shuffle 性能提升 | 低 | Shuffle |

### 中优先级（短期实施）

| 优化项 | 预期收益 | 实施难度 | 相关模块 |
|-------|---------|---------|---------|
| AggContext RowConverter 线程本地缓存 | 5-15% 聚合性能提升 | 中 | Aggregation |
| SortExec 类型特化 | 20-40% 排序性能 | 中 | Sort |
| 字符串函数 SIMD 优化 | 2-4x 字符串处理 | 中 | String Functions |
| AccPrimColumn 批量更新 | 10-20% 聚合性能 | 中 | Aggregation |

### 低优先级（长期规划）

| 优化项 | 预期收益 | 实施难度 | 相关模块 |
|-------|---------|---------|---------|
| JSON 函数批量解析 | 30-50% JSON 处理 | 高 | JSON Functions |
| 复杂类型哈希向量化 | 2-3x 复杂类型哈希 | 高 | Hash |
| 日期函数转换缓存 | 10-20% 日期处理 | 低 | Date Functions |
| TryCastExpr 标量优化 | 5-10% 转换性能 | 低 | Expression |
