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

//! Benchmark for Spark-compatible hash functions (datafusion-ext-commons/spark_hash.rs).
//!
//! Related to PERFORMANCE_OPTIMIZATION.md #8: hash_one has no batch path for
//! complex types; this benchmark establishes a baseline for the existing primitive
//! and string batch paths (create_murmur3_hashes / create_xxhash64_hashes).
//!
//! To run:
//!   cargo bench -p datafusion-ext-plans --bench spark_hash

use std::{hint::black_box, sync::Arc};

use arrow::{
    array::{Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_ext_commons::spark_hash::{create_murmur3_hashes, create_xxhash64_hashes};

const N: usize = 8192;

fn make_int32_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let col: Arc<dyn arrow::array::Array> =
        Arc::new(Int32Array::from_iter_values(0..N as i32));
    RecordBatch::try_new(schema, vec![col]).unwrap()
}

fn make_int64_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let col: Arc<dyn arrow::array::Array> =
        Arc::new(Int64Array::from_iter_values(0..N as i64));
    RecordBatch::try_new(schema, vec![col]).unwrap()
}

fn make_string_batch(str_len: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
    let strs: Vec<String> = (0..N)
        .map(|i| format!("{:0>width$}", i * 2654435761u64 as usize, width = str_len))
        .collect();
    let col: Arc<dyn arrow::array::Array> =
        Arc::new(StringArray::from(strs.iter().map(|s| s.as_str()).collect::<Vec<_>>()));
    RecordBatch::try_new(schema, vec![col]).unwrap()
}

fn bench_murmur3(c: &mut Criterion) {
    let mut group = c.benchmark_group("spark_hash/murmur3");

    let int32_batch = make_int32_batch();
    group.bench_function("int32_8k", |b| {
        b.iter(|| {
            black_box(create_murmur3_hashes(N, black_box(int32_batch.columns()), 42));
        });
    });

    let int64_batch = make_int64_batch();
    group.bench_function("int64_8k", |b| {
        b.iter(|| {
            black_box(create_murmur3_hashes(N, black_box(int64_batch.columns()), 42));
        });
    });

    for str_len in [8usize, 32, 128] {
        let str_batch = make_string_batch(str_len);
        group.bench_with_input(
            BenchmarkId::new("utf8_8k", format!("{str_len}B")),
            &str_len,
            |b, _| {
                b.iter(|| {
                    black_box(create_murmur3_hashes(
                        N,
                        black_box(str_batch.columns()),
                        42,
                    ));
                });
            },
        );
    }

    group.finish();
}

fn bench_xxhash64(c: &mut Criterion) {
    let mut group = c.benchmark_group("spark_hash/xxhash64");

    let int64_batch = make_int64_batch();
    group.bench_function("int64_8k", |b| {
        b.iter(|| {
            black_box(create_xxhash64_hashes(N, black_box(int64_batch.columns()), 42));
        });
    });

    for str_len in [8usize, 32, 128] {
        let str_batch = make_string_batch(str_len);
        group.bench_with_input(
            BenchmarkId::new("utf8_8k", format!("{str_len}B")),
            &str_len,
            |b, _| {
                b.iter(|| {
                    black_box(create_xxhash64_hashes(
                        N,
                        black_box(str_batch.columns()),
                        42,
                    ));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_murmur3, bench_xxhash64);
criterion_main!(benches);
