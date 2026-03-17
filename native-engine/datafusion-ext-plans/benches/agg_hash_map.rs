// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Benchmark for AggHashMap::upsert_records (agg/agg_hash_map.rs hot path).
//!
//! Covers two key optimization areas from PERFORMANCE_OPTIMIZATION.md:
//! - Issue #3: rehash SIMD partial usage
//! - Issue #7: MergingData entries 4-tuple size
//!
//! To run:
//!   cargo bench -p datafusion-ext-plans --bench agg_hash_map

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_ext_plans::agg::agg_hash_map::AggHashMap;
use rand::Rng;

fn make_keys(n: usize, key_len: usize) -> Vec<Vec<u8>> {
    let mut rng = rand::rng();
    (0..n)
        .map(|_| (0..key_len).map(|_| rng.random::<u8>()).collect())
        .collect()
}

fn bench_upsert_records(c: &mut Criterion) {
    let mut group = c.benchmark_group("agg_hash_map/upsert_records");

    // Typical aggregation key sizes: 4 bytes (i32), 8 bytes (i64), 16 bytes (two i64s)
    for &(n_rows, key_len) in &[
        (8192usize, 4usize),
        (8192, 8),
        (8192, 16),
        (65536, 8),
    ] {
        let keys = make_keys(n_rows, key_len);
        group.bench_with_input(
            BenchmarkId::new("all_distinct", format!("{n_rows}rows_{}B", key_len)),
            &(n_rows, key_len),
            |b, _| {
                let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                b.iter_batched(
                    || key_refs.clone(),
                    |key_refs| {
                        let mut map = AggHashMap::default();
                        black_box(map.upsert_records(black_box(key_refs)));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    // Re-insertion: same 100 keys repeated 8192 times → exercises the "found" branch
    // and stresses the SIMD hash_matched loop.
    {
        let keys = make_keys(100, 8);
        let repeated: Vec<&[u8]> = keys
            .iter()
            .cycle()
            .take(8192)
            .map(|k| k.as_slice())
            .collect();
        group.bench_function("repeated_keys_8k_8B", |b| {
            b.iter_batched(
                || repeated.clone(),
                |key_refs| {
                    let mut map = AggHashMap::default();
                    black_box(map.upsert_records(black_box(key_refs)));
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_upsert_records);
criterion_main!(benches);
