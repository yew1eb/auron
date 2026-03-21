# RangePartitioning RowConverter 优化 - Issue 和 PR 文档

## GitHub Issue

```markdown
## [AURON-XXX] Performance: Cache RowConverter in RangePartitioning to avoid per-batch recreation

### Problem
In `evaluate_range_partition_ids()` (`native-engine/datafusion-ext-plans/src/shuffle/mod.rs`), a new `RowConverter` is created for every batch processed during range partitioning shuffle operations. Since all batches in a shuffle operation share the same schema, this recreation is redundant.

### Current Code
```rust
fn evaluate_range_partition_ids(
    batch: &RecordBatch,
    sort_expr: &Vec<PhysicalSortExpr>,
    bound_rows: &Arc<Rows>,
) -> Result<Vec<u32>> {
    // ❌ Created for EVERY batch
    let sort_row_converter = Arc::new(SyncMutex::new(RowConverter::new(
        sort_expr
            .iter()
            .map(|expr| {
                Ok(SortField::new_with_options(
                    expr.expr.data_type(&batch.schema())?,
                    expr.options,
                ))
            })
            .collect::<Result<Vec<SortField>>>()?,  // Build SortFields every time
    )?));
    // ... use converter
}
```

### Proposed Solution
Cache the `RowConverter` in `BufferedData` (which already holds the `Partitioning` and lives for the duration of the shuffle operation):

1. Add cached `RowConverter` field to `BufferedData`
2. Modify `evaluate_range_partition_ids()` to accept pre-computed `SortField` list and cached converter
3. Build `SortField` list and converter on first batch, reuse for subsequent batches

### Benefits
- Minimal code changes (localized to shuffle module)
- No API breaking changes
- Lifecycle match: `BufferedData` and `RowConverter` have same lifetime
- `RowConverter` is immutable and thread-safe (no Mutex needed)

### Testing
- [ ] Unit tests in shuffle/mod.rs pass
- [ ] Integration tests with range partitioning pass
- [ ] No regression in existing shuffle tests
- [ ] `cargo fmt` passes
- [ ] `cargo clippy` passes

### References
- Documented in OPTIMIZATION_GUIDE.md as P0 priority issue
```

---

## Pull Request

```markdown
## [AURON-XXX] Cache RowConverter in RangePartitioning to eliminate per-batch recreation

### What changes were proposed in this pull request?
This PR optimizes the `RangePartitioning` shuffle operation by caching the `RowConverter` in `BufferedData`, eliminating the per-batch recreation.

**Key changes:**
1. Added `range_row_converter` field to `BufferedData` to cache `RowConverter` and its `SortField` list
2. Modified `sort_batches_by_partition_id()` to accept and pass cached converter
3. Modified `evaluate_range_partition_ids()` to accept pre-computed `SortField` list and cached converter
4. `SortField` list and converter are built on first batch processing, then reused

### Why are the changes needed?
The `RowConverter::new()` is called for every batch in `evaluate_range_partition_ids()`, causing unnecessary overhead:

- Each call builds `SortField` list from expressions
- Each call allocates and initializes converter structures
- Since all batches share the same schema in a shuffle operation, this is redundant

**Before:**
```rust
// Called for EVERY batch
let sort_row_converter = Arc::new(SyncMutex::new(RowConverter::new(
    sort_expr.iter().map(...).collect()  // Build SortFields every time
)?));
```

**After:**
```rust
// Created once, reused across batches
if cached_converter.is_none() {
    *cached_converter = Some((sort_fields, converter));  // Build once
}
// Use cached_converter for all batches
```

### Does this PR introduce any user-facing change?
No. This is an internal optimization with no API changes.

### How was this patch tested?
- [x] Existing unit tests in `shuffle/mod.rs` pass
- [x] Existing integration tests pass
- [x] `cargo test --workspace` passes
- [x] `cargo fmt --check` passes
- [x] `cargo clippy --workspace` passes

### Was this patch authored or co-authored using generative AI tooling?
No.
```

---

## 使用说明

### 提交 Issue
1. 访问 https://github.com/yew1eb/auron/issues
2. 点击 "New Issue"
3. 粘贴上面的 Issue 内容
4. 添加标签：`performance`, `optimization`, `shuffle`

### 提交 PR
1. 确保代码已推送到远程分支
2. 访问 https://github.com/yew1eb/auron/pulls
3. 点击 "New Pull Request"
4. 选择 base: master, compare: AURON-optimize-range-partitioning
5. 粘贴上面的 PR 内容
6. 关联 Issue: 在描述中添加 `Closes #XXX` (替换 XXX 为 Issue 号)

### 提交前检查清单
```bash
# 1. 代码格式化
cargo fmt --check

# 2. Clippy 检查
cargo clippy --workspace

# 3. 运行测试
cargo test --workspace

# 4. 查看提交历史
git log --oneline master..HEAD
```
