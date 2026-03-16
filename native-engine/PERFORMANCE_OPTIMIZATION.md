# Native Engine Performance Optimization Analysis

This document records performance optimization opportunities identified in the Auron native engine
Rust codebase, covering hash maps, shuffle, aggregation, sort, and join operators.

---

## Summary Table

| Priority | Issue | Location |
|----------|-------|----------|
| High | `RangePartitioning` rebuilds `RowConverter` on every batch | `datafusion-ext-plans/src/shuffle/mod.rs` |
| High | `radix_sort_by_key` outer loop has O(k²) `retain` call | `datafusion-ext-commons/src/algorithm/rdx_sort.rs` |
| Medium | `AggHashMap::rehash` only partially exploits SIMD | `datafusion-ext-plans/src/agg/agg_hash_map.rs` |
| Medium | `JoinHashMap::lookup_many` issues two SIMD comparisons per probe step | `datafusion-ext-plans/src/joins/join_hash_map.rs` |
| Medium | Shuffle staging `Vec` is allocated and dropped on every `flush_staging` | `datafusion-ext-plans/src/shuffle/buffered_data.rs` |
| Medium | BHJ probe: null-key filtering forces a `collect` + dual-cursor pattern | `datafusion-ext-plans/src/joins/bhj/full_join.rs` |
| Medium | `MergingData::entries` stores a 16-byte 4-tuple; can be compressed to 12 bytes | `datafusion-ext-plans/src/agg/agg_table.rs` |
| Low | `hash_one` has no batch path for `List`/`Map`/`Struct` columns | `datafusion-ext-commons/src/spark_hash.rs` |

---

## 1. `RangePartitioning` Rebuilds `RowConverter` on Every Batch (High)

**File:** `datafusion-ext-plans/src/shuffle/mod.rs:210`

```rust
fn evaluate_range_partition_ids(
    batch: &RecordBatch,
    sort_expr: &Vec<PhysicalSortExpr>,
    bound_rows: &Arc<Rows>,
) -> Result<Vec<u32>> {
    // ← Built fresh on every call
    let sort_row_converter = Arc::new(SyncMutex::new(RowConverter::new(
        sort_expr.iter()
            .map(|expr| Ok(SortField::new_with_options(...)))
            .collect::<Result<Vec<SortField>>>()?,
    )?));
    ...
}
```

**Problem:** `RowConverter::new` allocates internal state (field encoders, buffer pools). This
function is called for every incoming `RecordBatch`, so the allocation happens O(batches) times
even though the schema never changes.

**Suggested fix:** Cache the `RowConverter` inside `Partitioning::RangePartitioning` as an
additional field (e.g., `Arc<Mutex<RowConverter>>`), built once at construction time and reused
across batches.

---

## 2. `radix_sort_by_key` Outer Loop O(k²) `retain` (High)

**File:** `datafusion-ext-commons/src/algorithm/rdx_sort.rs:56`

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

**Problem:** `retain` scans the full `inexhausted_part_indices` slice every outer iteration.
For large partition counts (e.g. 1000-way shuffle), this becomes O(k²) in the worst case.
Each pass also causes multiple small scan passes over a Vec that starts at `k` and shrinks by
at least one per round.

**Suggested fix:** Replace `retain` with a swap-remove idiom or a separate done-flag bitset so
exhausted partitions are skipped in O(1) per entry rather than O(k) per round:

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
    // process active partitions ...
}
```

---

## 3. `AggHashMap::rehash` Partial SIMD Usage (Medium)

**File:** `datafusion-ext-plans/src/agg/agg_hash_map.rs:139`

```rust
fn rehash(&mut self, map_mod_bits: u32) {
    let mut rehashed_map = unchecked!(vec![MapValueGroup::default(); 1 << map_mod_bits]);
    for group in self.map.drain(..) {
        let new_entries = group.hashes % new_mods;   // SIMD mod for 8 hashes at once
        for &e in new_entries.as_array().iter().rev() {
            prefetch_write_data!(&rehashed_map[e as usize]);  // prefetch
        }
        // ← Insertion is still scalar open-addressing per element
        for j in 0..MAP_VALUE_GROUP_SIZE {
            if non_empty.test(j) { ... loop { ... } }
        }
    }
}
```

**Problem:** SIMD is used to compute the 8 target slot indices at once and to prefetch them,
but the actual insertion back into `rehashed_map` is a sequential open-addressing loop per
element. A hash-collision chain can cause multiple cache misses per element.

**Suggested fix:** During rehash, collect all `(hash, value)` pairs into a flat `Vec`, sort them
by `new_entries` (one more radix pass), then bulk-fill groups sequentially. This turns random
writes into mostly sequential writes and makes better use of the prefetch that was already there.

---

## 4. `JoinHashMap::lookup_many` Double SIMD Comparison per Probe Step (Medium)

**File:** `datafusion-ext-plans/src/joins/join_hash_map.rs:255`

```rust
loop {
    let hash_matched = self.map[e].hashes.simd_eq(Simd::splat(hashes[i]));
    let empty = self.map[e].hashes.simd_eq(Simd::splat(0));
    // Two simd_eq + one OR per probe step
    if let Some(pos) = (hash_matched | empty).first_set() { ... }
    e += 1;
}
```

**Problem:** Every probe step does two SIMD comparisons (`simd_eq`) unconditionally, then ORs
the results. In the common case where a hash collision is absent (the slot is empty on the first
probe), the `hash_matched` comparison is wasted.

**Suggested fix:** Match the pattern used in `agg_hash_map.rs::upsert_one_impl`—check
`hash_matched` first, break early on a hit; only fall through to `simd_eq(0)` if no match was
found in that group. This halves the SIMD work on the happy path (empty slot or direct hit):

```rust
loop {
    let hash_matched = self.map[e].hashes.simd_eq(Simd::splat(hashes[i]));
    if let Some(pos) = hash_matched.first_set() {
        // hit
        hashes[i] = transmute(self.map[e].values[pos]);
        break;
    }
    let empty = self.map[e].hashes.simd_eq(Simd::splat(0));
    if let Some(_) = empty.first_set() {
        // miss (empty slot reached)
        hashes[i] = transmute(MapValue::EMPTY);
        break;
    }
    e += 1;
    e %= 1 << self.map_mod_bits;
}
```

---

## 5. Shuffle Staging `Vec` Repeatedly Allocated (Medium)

**File:** `datafusion-ext-plans/src/shuffle/buffered_data.rs:285`

```rust
fn sort_batches_by_partition_id(...) -> Result<(Vec<u32>, RecordBatch)> {
    // ← Allocated every flush_staging call
    let mut partition_indices = batches.iter().enumerate()
        .flat_map(|(batch_idx, batch)| { ... })
        .collect::<Vec<_>>();

    let mut part_counts = vec![0; num_partitions];
    radix_sort_by_key(&mut partition_indices, &mut part_counts, ...);
    ...
}
```

**Problem:** `partition_indices` (a `Vec<(u32, u32, u32)>`) and `part_counts` (`Vec<usize>`) are
allocated, sorted, and dropped on every `flush_staging` call. For workloads with many small
batches this causes high allocator pressure on a hot path.

**Suggested fix:** Add two reusable scratch buffers (`staging_scratch_indices: Vec<(u32,u32,u32)>`
and `part_counts_scratch: Vec<usize>`) to `BufferedData`, clear them at the start of each
`flush_staging`, and pass them by `&mut` reference. This amortises the allocation across the
lifetime of the `BufferedData` object.

---

## 6. BHJ Probe: Null-Key Filtering Forces `collect` + Dual-Cursor (Medium)

**File:** `datafusion-ext-plans/src/joins/bhj/full_join.rs:243`

```rust
let map_values = probed_side_search_time.with_timer(|| {
    let probed_hashes = if let Some(probed_valids) = &probed_valids {
        probed_hashes.iter().enumerate()
            .filter_map(|(row_idx, &hash)| probed_valids.is_valid(row_idx).then_some(hash))
            .collect()   // ← allocates a shorter Vec
    } else {
        probed_hashes
    };
    map.lookup_many(probed_hashes)  // returns Vec len < num_rows when nulls present
});

// Later: advance hashes_idx only for valid rows via a separate counter
let mut hashes_idx = 0;
for row_idx in 0..probed_batch.num_rows() {
    if valid(row_idx) {
        let map_value = map_values[hashes_idx];
        hashes_idx += 1;
        ...
    }
}
```

**Problem:** The null-filtering collect creates an intermediate Vec and introduces the dual-cursor
(`row_idx` / `hashes_idx`) pattern in the hot probe loop, which prevents the compiler from
auto-vectorising the loop body.

**Suggested fix:** Always pass the full-length hash array to `lookup_many`. Null-key rows can use
hash value `0` (which maps to `MapValue::EMPTY` since the table guarantees non-zero hashes). Then
the probe loop becomes a simple single-cursor `for row_idx in 0..N` with an early-continue on
`probed_valids.is_null(row_idx)`, which is easier to optimise:

```rust
// Before lookup: zero out null positions instead of filtering
if let Some(pv) = &probed_valids {
    for (i, h) in probed_hashes.iter_mut().enumerate() {
        if pv.is_null(i) { *h = 0; }
    }
}
let map_values = map.lookup_many(probed_hashes); // always len == num_rows
```

---

## 7. `MergingData::entries` Oversized 4-Tuple (Medium)

**File:** `datafusion-ext-plans/src/agg/agg_table.rs:596`

```rust
entries: Vec<(u32, u32, u32, u32)>, // (bucket_id, batch_idx, row_idx, acc_idx)
```

**Problem:** Each entry is 16 bytes. `bucket_id` is capped at `num_spill_buckets` which is stored
as `u16` in the return type of `bucket_id()` (`agg_table.rs:838`), so the first field wastes
2 bytes. For millions of rows this amounts to tens of MB of unnecessary memory. Radix sort also
swaps full 16-byte entries.

**Suggested fix:** Change the type to `(u16, u16, u32, u32)` = 12 bytes, casting `bucket_id` to
`u16` on insertion. This also improves radix-sort swap performance because `swap_unchecked` on a
12-byte value touches fewer cache lines.

---

## 8. `hash_one` Has No Batch Path for `List`/`Map`/`Struct` (Low)

**File:** `datafusion-ext-commons/src/spark_hash.rs:218`

```rust
_ => {
    for idx in 0..array.len() {
        hash_one(array, idx, &mut hashes_buffer[idx], h);
    }
}
```

**Problem:** For complex columnar types (`List`, `Map`, `Struct`), `hash_one` is called once per
row with a recursive descent. This cannot be vectorised and causes repeated virtual dispatch
(`as_any().downcast_ref`) for every element.

**Suggested fix:**
- For `List<T>` where `T` is a primitive: flatten the values buffer, hash the contiguous
  sub-slice for each row using a single `spark_compatible_murmur3_hash` call, and fold it back
  into the row hash via offset arithmetic.
- For `Struct`: iterate over fields in a column-oriented pass (each field contributes to all row
  hashes in one sweep), mirroring the existing `hash_array` column loop.

This is a non-trivial change that may also benefit the upstream DataFusion project.
