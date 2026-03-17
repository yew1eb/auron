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

//! Benchmark for create_batch_interleaver (datafusion-ext-commons/arrow/selection.rs).
//!
//! Mirrors the SMJ FullJoiner::flush() call pattern described in
//! PERFORMANCE_OPTIMIZATION.md: the interleaver is created fresh on every flush.
//! This benchmark measures the cost of construction + invocation so that caching
//! the interleaver across flush calls can be quantified.
//!
//! To run:
//!   cargo bench -p datafusion-ext-plans --bench batch_interleaver

use std::{hint::black_box, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_ext_commons::arrow::selection::create_batch_interleaver;
use rand::Rng;

fn make_int64_batch(n_rows: usize, n_cols: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        (0..n_cols)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let cols: Vec<Arc<dyn arrow::array::Array>> = (0..n_cols)
        .map(|_| {
            Arc::new(Int64Array::from_iter_values(0..n_rows as i64))
                as Arc<dyn arrow::array::Array>
        })
        .collect();
    RecordBatch::try_new(schema, cols).unwrap()
}

fn bench_create_and_invoke(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_interleaver/create_and_invoke");
    let mut rng = rand::rng();

    // (src_rows, out_rows, n_cols) — typical SMJ output batch sizes
    for &(n_src_rows, n_out_rows, n_cols) in &[
        (4096usize, 4096usize, 4usize),
        (4096, 4096, 8),
        (16384, 4096, 4),
        (16384, 4096, 8),
    ] {
        let batch = make_int64_batch(n_src_rows, n_cols);
        let batches = vec![batch];
        let indices: Vec<(usize, usize)> = (0..n_out_rows)
            .map(|_| (0, rng.random::<u32>() as usize % n_src_rows))
            .collect();

        // Full cost: create interleaver + invoke (current SMJ flush behaviour)
        group.bench_with_input(
            BenchmarkId::new(
                "full_create_invoke",
                format!("{n_src_rows}src_{n_out_rows}out_{n_cols}cols"),
            ),
            &(),
            |b, _| {
                b.iter(|| {
                    let interleaver =
                        create_batch_interleaver(black_box(&batches), false).unwrap();
                    black_box(interleaver(black_box(&indices)).unwrap());
                });
            },
        );

        // Invoke-only cost: interleaver cached across calls (proposed optimisation)
        let cached_interleaver = create_batch_interleaver(&batches, false).unwrap();
        group.bench_with_input(
            BenchmarkId::new(
                "invoke_only_cached",
                format!("{n_src_rows}src_{n_out_rows}out_{n_cols}cols"),
            ),
            &(),
            |b, _| {
                b.iter(|| {
                    black_box(cached_interleaver(black_box(&indices)).unwrap());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_create_and_invoke);
criterion_main!(benches);
