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
//! ISOLATED BENCHMARK: Measures ONLY the RowConverter creation overhead
//! 
//! Key insight: The benefit comes from avoiding RowConverter::new() calls.
//! This benchmark isolates that specific operation to clearly show the savings.
//!
//! To run:
//!   cargo bench -p datafusion-ext-plans --bench range_partitioning

use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    row::{RowConverter, SortField},
};
use arrow_schema::SortOptions;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column};

/// Create a complex schema with multiple data types
fn create_complex_schema() -> Schema {
    Schema::new(vec![
        Field::new("int_col", DataType::Int64, false),
        Field::new("string_col", DataType::Utf8, false),
        Field::new("int_col2", DataType::Int64, false),
        Field::new("string_col2", DataType::Utf8, false),
    ])
}

/// Create sort expressions
fn create_sort_exprs() -> Vec<PhysicalSortExpr> {
    vec![
        PhysicalSortExpr {
            expr: Arc::new(Column::new("int_col", 0)),
            options: SortOptions::default(),
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("string_col", 1)),
            options: SortOptions::default(),
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("int_col2", 2)),
            options: SortOptions::default(),
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("string_col2", 3)),
            options: SortOptions::default(),
        },
    ]
}

/// ISOLATED TEST: Only measure RowConverter creation
fn bench_row_converter_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_converter_isolated");
    
    let schema = Arc::new(create_complex_schema());
    let sort_exprs = create_sort_exprs();
    
    // Different batch counts to simulate different scenarios
    for num_batches in [100, 500, 1000, 2000] {
        // ===== BASELINE: Create converter for EACH batch =====
        group.bench_with_input(
            BenchmarkId::new("create_per_batch", format!("{}_batches", num_batches)),
            &num_batches,
            |b, &n| {
                b.iter(|| {
                    for _ in 0..n {
                        // Simulate what happens in evaluate_range_partition_ids
                        // Build SortFields (requires schema lookup)
                        let sort_fields: Vec<SortField> = sort_exprs
                            .iter()
                            .map(|expr| {
                                SortField::new_with_options(
                                    expr.expr.data_type(&schema).unwrap(),
                                    expr.options,
                                )
                            })
                            .collect();
                        
                        // Create RowConverter (THE COST WE WANT TO MEASURE)
                        let _converter = RowConverter::new(sort_fields).unwrap();
                    }
                });
            },
        );

        // ===== OPTIMIZED: Create converter ONCE, reuse =====
        group.bench_with_input(
            BenchmarkId::new("create_once", format!("{}_batches", num_batches)),
            &num_batches,
            |b, &n| {
                // Pre-create converter (the optimization)
                let sort_fields: Vec<SortField> = sort_exprs
                    .iter()
                    .map(|expr| {
                        SortField::new_with_options(
                            expr.expr.data_type(&schema).unwrap(),
                            expr.options,
                        )
                    })
                    .collect();
                let _cached_converter = RowConverter::new(sort_fields).unwrap();

                b.iter(|| {
                    for _ in 0..n {
                        // In real code: reuse cached_converter
                        // Here we just verify it's already created
                        // This measures the ZERO-cost of reuse
                        std::hint::black_box(&_cached_converter);
                    }
                });
            },
        );
    }

    group.finish();
}

/// SINGLE CREATION benchmark: How long does ONE RowConverter creation take?
fn bench_single_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_converter_single_creation");
    
    let schema = Arc::new(create_complex_schema());
    let sort_exprs = create_sort_exprs();
    
    // Measure just ONE creation
    group.bench_function("complex_schema_4_fields", |b| {
        b.iter(|| {
            let sort_fields: Vec<SortField> = sort_exprs
                .iter()
                .map(|expr| {
                    SortField::new_with_options(
                        expr.expr.data_type(&schema).unwrap(),
                        expr.options,
                    )
                })
                .collect();
            RowConverter::new(sort_fields).unwrap()
        });
    });
    
    group.finish();
}

criterion_group!(benches, bench_row_converter_creation, bench_single_creation);
criterion_main!(benches);
