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

//! Benchmark for RangePartitioning RowConverter caching optimization.
//!
//! Measures the performance improvement from caching RowConverter across batches
//! vs creating a new one for each batch. This exposes the overhead documented
//! in OPTIMIZATION_GUIDE.md P0 issue: "RangePartitioning 每批次重建 RowConverter".
//!
//! To run:
//!   cargo bench -p datafusion-ext-plans --bench range_partitioning

use std::{hint::black_box, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
    row::{Row, RowConverter, Rows, SortField},
};
use arrow_schema::SortOptions;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column};
use parking_lot::Mutex as SyncMutex;

/// Create a test batch with Int64 columns
fn make_batch(n_rows: usize, offset: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Int64, false),
    ]));
    let cols: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Int64Array::from_iter_values(offset..offset + n_rows as i64)),
        Arc::new(Int64Array::from_iter_values((offset * 2)..(offset * 2 + n_rows as i64))),
    ];
    RecordBatch::try_new(schema, cols).unwrap()
}

/// Create sort expressions for testing
fn make_sort_exprs(_schema: &Schema) -> Vec<PhysicalSortExpr> {
    vec![
        PhysicalSortExpr {
            expr: Arc::new(Column::new("c1", 0)),
            options: SortOptions::default(),
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("c2", 1)),
            options: SortOptions::default(),
        },
    ]
}

/// Create bound rows for range partitioning (simulating partition boundaries)
fn make_bound_rows(n_partitions: usize) -> Arc<Rows> {
    let sort_fields = vec![
        SortField::new(DataType::Int64),
        SortField::new(DataType::Int64),
    ];
    let converter = RowConverter::new(sort_fields).unwrap();

    // Create boundary values: 0, 100, 200, ... for partition boundaries
    let bounds: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Int64Array::from_iter_values(
            (0..n_partitions - 1).map(|i| i as i64 * 100),
        )),
        Arc::new(Int64Array::from_iter_values(
            (0..n_partitions - 1).map(|i| i as i64 * 100),
        )),
    ];

    Arc::new(converter.convert_columns(&bounds).unwrap())
}

/// Simulate evaluate_range_partition_ids with a converter
fn evaluate_range_partition_ids_with_converter(
    batch: &RecordBatch,
    sort_exprs: &[PhysicalSortExpr],
    bound_rows: &Arc<Rows>,
    converter: &Arc<SyncMutex<RowConverter>>,
) -> Vec<u32> {
    use arrow::row::Row;

    let key_cols: Vec<Arc<dyn arrow::array::Array>> = sort_exprs
        .iter()
        .map(|expr| {
            expr.expr
                .evaluate(batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap()
        })
        .collect();

    let key_rows = converter.lock().convert_columns(&key_cols).unwrap();

    let mut result = Vec::with_capacity(batch.num_rows());
    for key_row in key_rows.iter() {
        let partition = binary_search_partition(bound_rows, key_row);
        result.push(partition);
    }
    result
}

fn binary_search_partition(bound_rows: &Rows, target: Row) -> u32 {
    let mut low = 0usize;
    let mut high = bound_rows.num_rows();

    while low < high {
        let mid = (low + high) / 2;
        if bound_rows.row(mid) < target {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low as u32
}

fn bench_range_partitioning(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_partitioning/evaluate_partition_ids");

    // Test scenarios: (batch_size, num_batches, num_partitions)
    for &(batch_size, num_batches, n_partitions) in &[
        (1024usize, 100usize, 10usize), // Small batches, many iterations
        (4096, 50, 10),                 // Medium batches
        (8192, 20, 100),                // Large batches, more partitions
        (1024, 200, 200), // Many small batches (high converter creation overhead)
    ] {
        let batch = make_batch(batch_size, 0);
        let schema = batch.schema();
        let sort_exprs = make_sort_exprs(&schema);
        let bound_rows = make_bound_rows(n_partitions);

        // Benchmark ID for clear output
        let bench_id = format!(
            "{}batches_{}rows_{}parts",
            num_batches, batch_size, n_partitions
        );

        // ===== BASELINE: Create RowConverter for each batch (current implementation) =====
        group.bench_with_input(
            BenchmarkId::new("per_batch_converter", &bench_id),
            &(batch_size, num_batches),
            |b, _| {
                b.iter(|| {
                    for i in 0..num_batches {
                        let batch = make_batch(batch_size, i as i64 * batch_size as i64);
                        // Simulate current behavior: create converter every time
                        let sort_fields: Vec<SortField> = sort_exprs
                            .iter()
                            .map(|expr| {
                                SortField::new_with_options(
                                    expr.expr.data_type(&schema).unwrap(),
                                    expr.options,
                                )
                            })
                            .collect();
                        let converter = Arc::new(SyncMutex::new(
                            RowConverter::new(sort_fields).unwrap(),
                        ));

                        // Call the function (simulated)
                        let _result = evaluate_range_partition_ids_with_converter(
                            black_box(&batch),
                            black_box(&sort_exprs),
                            black_box(&bound_rows),
                            black_box(&converter),
                        );
                    }
                });
            },
        );

        // ===== OPTIMIZED: Cache RowConverter across batches =====
        group.bench_with_input(
            BenchmarkId::new("cached_converter", &bench_id),
            &(batch_size, num_batches),
            |b, _| {
                // Pre-create converter (optimization)
                let sort_fields: Vec<SortField> = sort_exprs
                    .iter()
                    .map(|expr| {
                        SortField::new_with_options(
                            expr.expr.data_type(&schema).unwrap(),
                            expr.options,
                        )
                    })
                    .collect();
                let cached_converter =
                    Arc::new(SyncMutex::new(RowConverter::new(sort_fields).unwrap()));

                b.iter(|| {
                    for i in 0..num_batches {
                        let batch = make_batch(batch_size, i as i64 * batch_size as i64);
                        // Use cached converter
                        let _result = evaluate_range_partition_ids_with_converter(
                            black_box(&batch),
                            black_box(&sort_exprs),
                            black_box(&bound_rows),
                            black_box(&cached_converter),
                        );
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_range_partitioning);
criterion_main!(benches);
