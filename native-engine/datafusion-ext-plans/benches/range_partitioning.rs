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
//! This benchmark is designed to AMPLIFY the RowConverter creation overhead
//! to clearly demonstrate the optimization benefit:
//!
//! 1. Uses STRING type (more complex than Int64, requires more setup in RowConverter)
//! 2. Uses MANY small batches (500-1000 batches) to maximize creation frequency
//! 3. Compares per-batch creation vs cached converter
//!
//! To run:
//!   cargo bench -p datafusion-ext-plans --bench range_partitioning

use std::{hint::black_box, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
    row::{Row, RowConverter, Rows, SortField},
};
use arrow_schema::SortOptions;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column};

/// Create a test batch with STRING columns (more complex than Int64)
/// String type requires more complex RowConverter setup
fn make_string_batch(n_rows: usize, batch_idx: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s1", DataType::Utf8, false),
        Field::new("s2", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));
    
    let base = batch_idx * n_rows;
    let cols: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(StringArray::from(
            (0..n_rows)
                .map(|i| format!("user_{:08}", base + i))
                .collect::<Vec<_>>()
        )),
        Arc::new(StringArray::from(
            (0..n_rows)
                .map(|i| format!("region_{:04}", (base + i) % 1000))
                .collect::<Vec<_>>()
        )),
        Arc::new(Int64Array::from_iter_values(
            (base as i64)..(base as i64 + n_rows as i64)
        )),
    ];
    RecordBatch::try_new(schema, cols).unwrap()
}

/// Create sort expressions for testing
fn make_sort_exprs(_schema: &Schema) -> Vec<PhysicalSortExpr> {
    vec![
        PhysicalSortExpr {
            expr: Arc::new(Column::new("s1", 0)),
            options: SortOptions::default(),
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("s2", 1)),
            options: SortOptions::default(),
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("id", 2)),
            options: SortOptions::default(),
        },
    ]
}

/// Create bound rows for range partitioning
fn make_bound_rows(n_partitions: usize) -> Arc<Rows> {
    let sort_fields = vec![
        SortField::new(DataType::Utf8),
        SortField::new(DataType::Utf8),
        SortField::new(DataType::Int64),
    ];
    let converter = RowConverter::new(sort_fields).unwrap();

    // Create boundary values
    let bounds: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(StringArray::from(
            (0..n_partitions - 1)
                .map(|i| format!("user_{:08}", i * 100))
                .collect::<Vec<_>>()
        )),
        Arc::new(StringArray::from(
            (0..n_partitions - 1)
                .map(|i| format!("region_{:04}", i % 100))
                .collect::<Vec<_>>()
        )),
        Arc::new(Int64Array::from_iter_values(
            (0..n_partitions - 1).map(|i| i as i64 * 100)
        )),
    ];

    Arc::new(converter.convert_columns(&bounds).unwrap())
}

/// Evaluate partition IDs using a converter
fn evaluate_range_partition_ids_with_converter(
    batch: &RecordBatch,
    sort_exprs: &[PhysicalSortExpr],
    bound_rows: &Arc<Rows>,
    converter: &Arc<RowConverter>,
) -> Vec<u32> {
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

    let key_rows = converter.convert_columns(&key_cols).unwrap();

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
    // Use longer measurement time for more stable results
    let mut group = c.benchmark_group("range_partitioning/evaluate_partition_ids");
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(100);

    // Test scenarios designed to AMPLIFY RowConverter creation overhead:
    // - Small batch size: reduces data processing time, making creation overhead more visible
    // - Many batches: increases creation frequency
    // - String type: more complex RowConverter setup than primitive types
    for &(batch_size, num_batches, n_partitions) in &[
        (100usize, 500usize, 50usize),   // Many tiny batches (creation overhead dominant)
        (200, 300, 100),                 // Medium batches, more partitions
        (50, 1000, 20),                  // Extreme: 1000 tiny batches
    ] {
        let batch = make_string_batch(batch_size, 0);
        let schema = batch.schema();
        let sort_exprs = make_sort_exprs(&schema);
        let bound_rows = make_bound_rows(n_partitions);

        let bench_id = format!(
            "{}b_{}r_{}p",
            num_batches, batch_size, n_partitions
        );

        // ===== BASELINE: Create RowConverter for each batch =====
        group.bench_with_input(
            BenchmarkId::new("per_batch_create", &bench_id),
            &(batch_size, num_batches),
            |b, _| {
                b.iter(|| {
                    for i in 0..num_batches {
                        let batch = make_string_batch(batch_size, i);
                        
                        // Build SortFields from scratch (schema lookup)
                        let sort_fields: Vec<SortField> = sort_exprs
                            .iter()
                            .map(|expr| {
                                SortField::new_with_options(
                                    expr.expr.data_type(&schema).unwrap(),
                                    expr.options,
                                )
                            })
                            .collect();
                        
                        // Create RowConverter (THE EXPENSIVE PART)
                        let converter = Arc::new(
                            RowConverter::new(sort_fields).unwrap(),
                        );

                        // Use the converter
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
            BenchmarkId::new("cached_reuse", &bench_id),
            &(batch_size, num_batches),
            |b, _| {
                // Pre-create converter ONCE (the optimization)
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
                    Arc::new(RowConverter::new(sort_fields).unwrap());

                b.iter(|| {
                    for i in 0..num_batches {
                        let batch = make_string_batch(batch_size, i);
                        // Reuse cached converter (NO CREATION OVERHEAD)
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
