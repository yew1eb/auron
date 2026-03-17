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

//! Benchmark for radix_sort_by_key (datafusion-ext-commons/algorithm/rdx_sort.rs).
//!
//! Exposes the O(k²) `retain` loop issue documented in PERFORMANCE_OPTIMIZATION.md #2.
//! High partition counts (k=1000) magnify the cost significantly relative to k=50.
//!
//! To run:
//!   cargo bench -p datafusion-ext-plans --bench rdx_sort

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_ext_commons::algorithm::rdx_sort::radix_sort_by_key;
use rand::Rng;

fn bench_radix_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("rdx_sort/radix_sort_by_key");
    let mut rng = rand::rng();

    for &(n_items, n_partitions) in &[
        (1_000_000usize, 50usize),
        (1_000_000, 200),
        (1_000_000, 1000),
        (500_000, 200),
    ] {
        let data: Vec<u32> = (0..n_items)
            .map(|_| rng.random::<u32>() % n_partitions as u32)
            .collect();

        group.bench_with_input(
            BenchmarkId::new(
                "uniform",
                format!("{}items_{}parts", n_items, n_partitions),
            ),
            &(n_items, n_partitions),
            |b, _| {
                b.iter_batched(
                    || (data.clone(), vec![0usize; n_partitions]),
                    |(mut arr, mut counts)| {
                        radix_sort_by_key(
                            black_box(&mut arr),
                            black_box(&mut counts),
                            |v| *v as usize,
                        );
                        black_box((arr, counts));
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    // Skewed: 90% of items fall into the first 10% of partitions.
    // This makes most partitions exhaust early → retain does more work.
    {
        let n_items = 1_000_000usize;
        let n_partitions = 200usize;
        let hot = (n_partitions / 10).max(1);
        let data: Vec<u32> = (0..n_items)
            .map(|_| {
                if rng.random::<f32>() < 0.9 {
                    rng.random::<u32>() % hot as u32
                } else {
                    hot as u32 + rng.random::<u32>() % (n_partitions - hot) as u32
                }
            })
            .collect();
        group.bench_function("skewed_1M_200parts", |b| {
            b.iter_batched(
                || (data.clone(), vec![0usize; n_partitions]),
                |(mut arr, mut counts)| {
                    radix_sort_by_key(
                        black_box(&mut arr),
                        black_box(&mut counts),
                        |v| *v as usize,
                    );
                    black_box((arr, counts));
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_radix_sort);
criterion_main!(benches);
