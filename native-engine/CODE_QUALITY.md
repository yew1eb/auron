# Native Engine 代码质量优化分析

本文档记录了在 Auron native engine Rust 代码库全量审查中发现的**非性能类**优化点，涵盖正确性缺陷、安全隐患、代码质量和 API 设计等维度。

---

## 汇总表

| 类别 | 严重程度 | 问题描述 | 所在文件 |
|------|----------|----------|----------|
| 正确性 | 高 | `AccCountColumn::mem_used()` 内存计算多乘了 2 倍 | `datafusion-ext-plans/src/agg/count.rs` |
| 正确性 | 高 | `MergingData::mem_used()` 对排序索引内存计算同样乘以 2 倍 | `datafusion-ext-plans/src/agg/agg_table.rs` |
| 正确性 | 中 | `JoinType` 转换遇到 `LeftMark`/`RightMark` 直接 `unreachable!()` 而非返回错误 | `datafusion-ext-plans/src/joins/join_utils.rs` |
| 正确性 | 中 | `batch_size()` 使用 `const OnceCell` 导致每次调用都是新实例，缓存无效 | `datafusion-ext-commons/src/lib.rs` |
| 正确性 | 中 | `AggProcessor::process_batch` 每行分区变更时分配新 `AccColumn`，开销极高 | `datafusion-ext-plans/src/window/processors/agg_processor.rs` |
| 安全性 | 高 | `SliceAsRawBytes` 的生命周期 `'a` 与 `&self` 解耦，允许调用方任意延长生命周期 | `datafusion-ext-commons/src/lib.rs` |
| 安全性 | 中 | `PartitionedBatchesIterator::next_partition_chunk` 用 `transmute` 绕过生命周期检查 | `datafusion-ext-plans/src/shuffle/buffered_data.rs` |
| 安全性 | 中 | `IpcReaderExec` 中使用 `assert!` 替代错误返回，断言失败会 panic | `datafusion-ext-plans/src/ipc_reader_exec.rs` |
| 代码质量 | 中 | `maxmin.rs::partial_merge` Boolean 分支在 `idx_for_zipped!` 内部重复 downcast | `datafusion-ext-plans/src/agg/maxmin.rs` |
| 代码质量 | 中 | `count.rs::partial_update` 在 `ensure_size` 之后仍有冗余越界检查 | `datafusion-ext-plans/src/agg/count.rs` |
| 代码质量 | 低 | `AggProcessor::process_batch` 将每行结果 push 到 `Vec` 再 `coalesce`，中间分配多余 | `datafusion-ext-plans/src/window/processors/agg_processor.rs` |
| 代码质量 | 低 | `WindowExec::with_output_window_cols` 及 `execute` 中重复构造 `WindowContext` | `datafusion-ext-plans/src/window_exec.rs` |
| 代码质量 | 低 | `GenerateExec::with_outer` 注释为 "only for testing" 但未用测试属性隔离 | `datafusion-ext-plans/src/generate_exec.rs` |
| 代码质量 | 低 | 多处 `statistics()` 实现返回 `todo!()`，上层依赖统计信息的优化器会 panic | 多处 `*_exec.rs` |

---

## 1. `AccCountColumn::mem_used()` 内存计算多乘 2 倍（正确性·高）

**文件：** `datafusion-ext-plans/src/agg/count.rs:184`

```rust
fn mem_used(&self) -> usize {
    self.values.capacity() * 2 * size_of::<i64>()
    //                      ^^^
    //  应为 1，乘以 2 导致汇报的内存是实际的两倍
}
```

**问题：** `AccCountColumn` 仅持有一个 `Vec<i64>`，正确的内存用量是 `capacity * size_of::<i64>()`。多余的 `* 2` 会导致内存管理器（`auron-memmgr`）误以为占用更多内存，触发不必要的 spill，降低聚合性能并可能使内存预算决策出现偏差。

**建议修复：**
```rust
fn mem_used(&self) -> usize {
    self.values.capacity() * size_of::<i64>()
}
```

---

## 2. `MergingData::mem_used()` 排序索引内存同样多乘 2 倍（正确性·高）

**文件：** `datafusion-ext-plans/src/agg/agg_table.rs:629`

```rust
fn mem_used(&self) -> usize {
    ...
    mem_used += // sorting indices memory usage
        self.entries.capacity() * 2 * size_of::<(u32, u32, u32, u32)>();
    //                            ^^^  同样多乘了 2 倍
    ...
}
```

**问题：** `entries` 是一个 `Vec<(u32,u32,u32,u32)>`，每个元素 16 字节。`* 2` 导致该字段内存估算虚高 2 倍，与第 1 条问题性质相同。spill 触发阈值会偏低，造成提前 spill。

**建议修复：**
```rust
mem_used += self.entries.capacity() * size_of::<(u32, u32, u32, u32)>();
```

---

## 3. `JoinType` 转换 `unreachable!()` 应改为返回错误（正确性·中）

**文件：** `datafusion-ext-plans/src/joins/join_utils.rs:63`

```rust
impl TryFrom<datafusion::prelude::JoinType> for JoinType {
    fn try_from(value: datafusion::prelude::JoinType) -> Result<Self> {
        match value {
            ...
            datafusion::prelude::JoinType::LeftMark  => unreachable!(),
            datafusion::prelude::JoinType::RightMark => unreachable!(),
        }
    }
}
```

**问题：** 该函数的返回类型是 `Result<Self>`，应对所有输入返回 `Ok` 或 `Err`。当 DataFusion 未来支持 `LeftMark`/`RightMark` Join 并将其传入时，`unreachable!()` 会直接 panic 导致任务崩溃，而非优雅地返回"不支持"错误。

**建议修复：**
```rust
datafusion::prelude::JoinType::LeftMark  =>
    df_execution_err!("unsupported join type: LeftMark"),
datafusion::prelude::JoinType::RightMark =>
    df_execution_err!("unsupported join type: RightMark"),
```

---

## 4. `batch_size()` 使用 `const OnceCell` 缓存无效（正确性·中）

**文件：** `datafusion-ext-commons/src/lib.rs:74`

```rust
pub fn batch_size() -> usize {
    const CACHED_BATCH_SIZE: OnceCell<usize> = OnceCell::new();
    //    ^^^^^^^^^^^^^^^^^^
    //    const 在每次调用时都是全新的 OnceCell 实例！
    *CACHED_BATCH_SIZE.get_or_init(|| BATCH_SIZE.value().unwrap_or(10000) as usize)
}
```

**问题：** Rust 的 `const` 是编译期常量，每次函数调用时 `CACHED_BATCH_SIZE` 都是一个全新的空 `OnceCell`，并不在调用之间共享状态。因此每次调用 `batch_size()` 都会重新读取配置（`BATCH_SIZE.value()`），完全没有缓存效果。同一文件中 `suggested_batch_mem_size()` 等函数正确地使用了 `static`。

**建议修复：**
```rust
pub fn batch_size() -> usize {
    static CACHED_BATCH_SIZE: OnceCell<usize> = OnceCell::new();
    //     ^^^^^^
    *CACHED_BATCH_SIZE.get_or_init(|| BATCH_SIZE.value().unwrap_or(10000) as usize)
}
```

---

## 5. `AggProcessor::process_batch` 每行分区变更时重新分配 AccColumn（正确性·中）

**文件：** `datafusion-ext-plans/src/window/processors/agg_processor.rs:73`

```rust
for row_idx in 0..batch.num_rows() {
    ...
    if !same_partition {
        self.acc_col = self.agg.create_acc_column(1);  // ← 分区切换时重新分配
    }
    self.agg.partial_update(...)?;
    output.push(self.agg.final_merge(&mut self.acc_col, IdxSelection::Single(0))?);
}
```

**问题：** 每当分区边界发生变化，`create_acc_column(1)` 就会分配一个新的累加列。对于分区数量很多（每行一个分区）的极端情况，这会产生大量小对象分配。此外，`AccColumn` trait 已经有 `resize` 方法，可以原地重置而无需重新分配。

**建议修复：** 增加 `AccColumn::reset()` 方法（将内容清零/置为初始值），分区切换时调用 reset 而非重新分配：
```rust
if !same_partition {
    self.acc_col.reset();  // 原地清零，复用现有分配
}
```

---

## 6. `SliceAsRawBytes` 生命周期悬垂风险（安全性·高）

**文件：** `datafusion-ext-commons/src/lib.rs:204`

```rust
pub trait SliceAsRawBytes {
    fn as_raw_bytes<'a>(&self) -> &'a [u8];
    fn as_raw_bytes_mut<'a>(&mut self) -> &'a mut [u8];
}

impl<T: Sized + Copy> SliceAsRawBytes for [T] {
    fn as_raw_bytes<'a>(&self) -> &'a [u8] {
        let bytes_ptr = self.as_ptr() as *const u8;
        unsafe {
            std::slice::from_raw_parts(bytes_ptr, std::mem::size_of_val(self))
        }
    }
    ...
}
```

**问题：** 返回值的生命周期 `'a` 与 `&self` 的生命周期完全无关。这意味着调用方可以获取一个生命周期任意长的字节切片引用，而底层 slice 可能已被释放。这是典型的 Use-After-Free 风险。Rust 的生命周期规则旨在防止此类问题，但此处的 `'a` 绑定绕过了该保护。

**建议修复：** 将返回生命周期与 `&self` 的生命周期绑定：
```rust
pub trait SliceAsRawBytes {
    fn as_raw_bytes(&self) -> &[u8];
    fn as_raw_bytes_mut(&mut self) -> &mut [u8];
}

impl<T: Sized + Copy> SliceAsRawBytes for [T] {
    fn as_raw_bytes(&self) -> &[u8] {
        let bytes_ptr = self.as_ptr() as *const u8;
        unsafe {
            std::slice::from_raw_parts(bytes_ptr, std::mem::size_of_val(self))
        }
    }
    fn as_raw_bytes_mut(&mut self) -> &mut [u8] {
        let bytes_ptr = self.as_mut_ptr() as *mut u8;
        unsafe {
            std::slice::from_raw_parts_mut(bytes_ptr, std::mem::size_of_val(self))
        }
    }
}
```
此修改会使生命周期由编译器正确推导（与 `&self` 相同），需要检查所有调用方是否需要适配。

---

## 7. `PartitionedBatchesIterator::next_partition_chunk` 用 `transmute` 绕过生命周期（安全性·中）

**文件：** `datafusion-ext-plans/src/shuffle/buffered_data.rs:254`

```rust
pub fn next_partition_chunk(
    &mut self,
) -> Option<(usize, impl Iterator<Item = RecordBatch> + 'a)> {
    // safety: bypass lifetime checker
    let batches_iter =
        unsafe { std::mem::transmute::<_, &mut PartitionedBatchesIterator<'a>>(self) };
    ...
}
```

**问题：** 使用 `transmute` 将 `&mut self`（生命周期匿名）强制转换为 `&mut PartitionedBatchesIterator<'a>`（生命周期为 `'a`），目的是让返回的迭代器捕获 `&mut batches_iter`。这样做绕过了借用检查器，若调用方持有返回的迭代器同时再次调用 `next_partition_chunk`，会造成两个可变引用同时存在（UB）。

注释仅说"bypass lifetime checker"，缺少充分的安全论证。代码中有运行时检查（检查上一个 chunk 是否耗尽）但这不足以保证内存安全。

**建议修复：** 使用流式 API 将 `PartitionedBatchesIterator` 设计为单次消费的迭代器，或将 `batch_interleaver` 包装在 `Rc<RefCell<>>` 中明确共享，从而消除 `transmute` 的使用。

---

## 8. `IpcReaderExec::execute` 中断言失败导致 panic（安全性·中）

**文件：** `datafusion-ext-plans/src/ipc_reader_exec.rs:148`

```rust
let blocks_provider = jni_call_static!(
    JniBridge.getResource(...) -> JObject
)?;
assert!(!blocks_provider.as_obj().is_null());  // ← panic 路径

let blocks_local = jni_call!(ScalaFunction0(blocks_provider.as_obj()).apply() -> JObject)?;
assert!(!blocks_local.as_obj().is_null());     // ← panic 路径
```

**问题：** 如果 Java 侧返回 null（如资源已被回收、或 Java 侧异常），这里会直接 panic，而非返回 `Result::Err`。`panic` 会跨越 `tokio` task 边界，可能导致整个 worker 崩溃而非单个任务失败。项目本身禁止 `unwrap_used` 和 `panic`（见 CLAUDE.md），但这里的 `assert!` 效果等同。

**建议修复：**
```rust
if blocks_provider.as_obj().is_null() {
    return df_execution_err!("IpcReaderExec: blocks_provider resource is null");
}
```

---

## 9. `maxmin.rs::partial_merge` Boolean 分支内冗余 downcast（代码质量·中）

**文件：** `datafusion-ext-plans/src/agg/maxmin.rs:229`

```rust
macro_rules! handle_boolean {
    () => {{
        let accs = downcast_any!(accs, mut AccBooleanColumn)?;           // ← 外层 downcast
        let merging_accs = downcast_any!(merging_accs, mut AccBooleanColumn)?;
        idx_for_zipped! {
            ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                let accs = downcast_any!(accs, mut AccBooleanColumn)?;        // ← 内层重复 downcast
                let merging_accs = downcast_any!(merging_accs, mut AccBooleanColumn)?;
                ...
            }
        }
    }};
}
```

**问题：** 在宏展开后，`idx_for_zipped!` 内部的 `let accs = downcast_any!(...)` 遮蔽了外层已经 downcast 好的变量，造成每次迭代都执行一次多余的 downcast（即使 `downcast_any!` 使用的是 `downcast_ref` 开销极低，也是不必要的冗余，且会遮蔽外层变量引发混乱）。

**建议修复：** 删除 `idx_for_zipped!` 宏内部的两行重复 downcast：
```rust
macro_rules! handle_boolean {
    () => {{
        let accs = downcast_any!(accs, mut AccBooleanColumn)?;
        let merging_accs = downcast_any!(merging_accs, mut AccBooleanColumn)?;
        idx_for_zipped! {
            ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                // 直接使用外层已 downcast 的 accs / merging_accs
                if let Some(merging_value) = merging_accs.value(merging_acc_idx) {
                    ...
                }
            }
        }
    }};
}
```

---

## 10. `count.rs::partial_update` 冗余越界检查（代码质量·中）

**文件：** `datafusion-ext-plans/src/agg/count.rs:96`

```rust
fn partial_update(...) -> Result<()> {
    let accs = downcast_any!(accs, mut AccCountColumn)?;
    accs.ensure_size(acc_idx);  // ← 已保证 acc_idx < accs.values.len()

    if partial_args.is_empty() {
        idx_for_zipped! {
            ((acc_idx, _partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                if acc_idx >= accs.values.len() {   // ← ensure_size 之后仍做越界检查
                    accs.values.push(1);
                } else {
                    accs.values[acc_idx] += 1;
                }
            }
        }
    }
    ...
}
```

**问题：** `ensure_size(acc_idx)` 已经保证 `accs.values` 足够大，`idx_for_zipped!` 内的 `if acc_idx >= accs.values.len()` 分支永远不会为真，且 `push(1)` 的语义也不正确（应赋值为 1 而非追加）。这个冗余检查遗留自旧版实现，应清理。

**建议修复：**
```rust
accs.ensure_size(acc_idx);
idx_for_zipped! {
    ((acc_idx, _partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
        accs.values[acc_idx] += 1;
    }
}
```

---

## 11. `AggProcessor::process_batch` 中间 Vec 分配（代码质量·低）

**文件：** `datafusion-ext-plans/src/window/processors/agg_processor.rs:50`

```rust
let mut output = vec![];
for row_idx in 0..batch.num_rows() {
    ...
    output.push(
        self.agg.final_merge(&mut self.acc_col, IdxSelection::Single(0))?,
    );
}
Ok(Arc::new(coalesce_arrays_unchecked(self.agg.data_type(), &output)))
```

**问题：** `output` 是一个 `Vec<ArrayRef>`，每次 `final_merge` 都会分配一个单元素 `ArrayRef`，最后再通过 `coalesce_arrays_unchecked` 合并。对于 `batch.num_rows() = 8192` 的批次，这会产生 8192 次小数组分配。

**建议修复：** 使用 `ArrayBuilder` 累积结果，在循环结束后一次性 `finish()`，完全避免中间数组分配。

---

## 12. `WindowExec::with_output_window_cols` 及 `execute` 中重复构造 `WindowContext`（代码质量·低）

**文件：** `datafusion-ext-plans/src/window_exec.rs:93, 166`

```rust
pub fn with_output_window_cols(&self, output_window_cols: bool) -> Self {
    Self {
        context: Arc::new(
            WindowContext::try_new(  // ← 重新构造整个 context
                self.input.schema(),
                self.context.window_exprs.clone(),
                ...
            ).expect("failed to create window context"),
        ),
        ...
    }
}
```

以及 `execute` 中合并 `WindowGroupLimitExec` 时也重复构造：

```rust
let combined = Arc::new(Self {
    context: Arc::new(WindowContext::try_new(  // ← 第三处重新构造
        self.input.schema(),
        self.context.window_exprs.clone(),
        ...
    )?),
    ...
});
```

**问题：** `WindowContext::try_new` 会重新分配 `partition_row_converter` 和 `order_row_converter`（两个 `RowConverter`，含分配开销），每次都丢弃已有的 context。

**建议修复：** 为 `WindowContext` 添加 `with_output_window_cols(bool) -> Self` 方法，只更新 `output_window_cols` 字段和 `output_schema`，复用现有的 row converter。

---

## 13. `GenerateExec::with_outer` 测试专用方法未隔离（代码质量·低）

**文件：** `datafusion-ext-plans/src/generate_exec.rs:101`

```rust
/// only for testing
pub fn with_outer(&self, outer: bool) -> Self {
    Self::try_new(...)
        .expect("GeneratorExec::try_new failed")
}
```

**问题：** 该方法注释声明仅用于测试，但：
1. 使用了 `pub` 可见性，外部代码可以调用
2. 内部使用了 `expect` panic，在生产环境中不安全
3. 没有 `#[cfg(test)]` 属性隔离

**建议修复：**
```rust
#[cfg(test)]
pub fn with_outer(&self, outer: bool) -> Self {
    Self::try_new(
        self.input.clone(),
        self.generator.clone(),
        self.required_child_output_cols.clone(),
        self.generator_output_schema.clone(),
        outer,
    )
    .expect("GeneratorExec::try_new failed")
}
```

---

## 14. 多处 `statistics()` 返回 `todo!()` 导致潜在 panic（代码质量·低）

**涉及文件：**
- `datafusion-ext-plans/src/window_exec.rs:198`
- `datafusion-ext-plans/src/generate_exec.rs:186`
- `datafusion-ext-plans/src/ipc_reader_exec.rs:161`
- `datafusion-ext-plans/src/ipc_writer_exec.rs:130`
- `datafusion-ext-plans/src/ffi_reader_exec.rs:139`
- 以及其他多个 `*_exec.rs`

```rust
fn statistics(&self) -> Result<Statistics> {
    todo!()
}
```

**问题：** `todo!()` 宏会直接 panic。当 DataFusion 的统计信息推断框架（用于基于代价的优化）调用 `statistics()` 时，会使整个任务崩溃。目前 DataFusion 可能不总是调用它，但随着版本升级，依赖 `statistics()` 的优化路径会越来越多。

**建议修复：** 至少返回一个空统计信息，而非 panic：
```rust
fn statistics(&self) -> Result<Statistics> {
    Ok(Statistics::new_unknown(&self.schema()))
}
```

---

## 附录：关联 issue 参考

| 问题 | 建议优先级 | 备注 |
|------|-----------|------|
| `const OnceCell` → `static OnceCell` (`batch_size`) | 立即修复 | 一行改动，影响全局 batch size 缓存 |
| `AccCountColumn::mem_used` × 2 bug | 立即修复 | 影响 spill 决策 |
| `MergingData::mem_used` × 2 bug | 立即修复 | 影响 spill 决策 |
| `SliceAsRawBytes` 生命周期解耦 | 短期 | 需要检查所有调用方 |
| `JoinType unreachable!` → `df_execution_err!` | 短期 | 防御性编程 |
| `assert!` → 错误返回（IpcReaderExec）| 短期 | 防止 worker 崩溃 |
| `maxmin.rs` 冗余 downcast | 清理 | 可与下次 maxmin 改动一起提交 |
| `count.rs` 冗余越界检查 | 清理 | 一行改动 |
| `statistics()` 返回 `todo!()` | 长期 | 可随各算子完善逐步补充 |
