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

use std::hint::black_box;
use std::sync::Arc;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::expressions::{Column};

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use arrow::array::{Date32Builder, Decimal128Builder, Int32Builder, StringBuilder};
use arrow::datatypes::Field;
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion_ext_plans::shuffle::Partitioning;
use datafusion_ext_plans::shuffle_writer_exec::ShuffleWriterExec;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::TaskContext;
use futures_util::StreamExt;
use tokio::runtime::Runtime;
use auron_memmgr::MemManager;

fn criterion_benchmark(c: &mut Criterion) {
    MemManager::init(1073741824 * 2);
    let mut group = c.benchmark_group("shuffle_writer");

    let rt = Runtime::new().unwrap();

    group.bench_function("shuffle_writer: end to end", |b| {
        let exec = create_shuffle_writer_exec();
        b.to_async(&rt).iter(|| {
            black_box(async {
                let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
                while let Some(batch) = stream.next().await {
                    let _batch = batch.unwrap();
                }
            })
        })
    });
    group.finish();
}

fn create_shuffle_writer_exec(
) -> ShuffleWriterExec {
    let batches = create_batches(81920, 10);
    let schema = batches[0].schema();
    let partitions = &[batches];
    let input = Arc::new(TestMemoryExec::try_new(
        partitions,
        schema.clone(),
        None,
    ).unwrap());

    let partitioning = Partitioning::HashPartitioning(vec![Arc::new(Column::new("c0", 0))], 16);

    ShuffleWriterExec::try_new(
        input,
        partitioning,
        "/tmp/data.out".to_string(),
        "/tmp/index.out".to_string()
    ).unwrap()
}

fn create_batches(size: usize, count: usize) -> Vec<RecordBatch> {
    let batch = create_batch(size, true);
    let mut batches = Vec::new();
    for _ in 0..count {
        batches.push(batch.clone());
    }
    batches
}


fn create_batch(num_rows: usize, allow_nulls: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, true),
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Date32, true),
        Field::new("c3", DataType::Decimal128(11, 2), true),
    ]));
    let mut a = Int32Builder::new();
    let mut b = StringBuilder::new();
    let mut c = Date32Builder::new();
    let mut d = Decimal128Builder::new()
        .with_precision_and_scale(11, 2)
        .unwrap();
    for i in 0..num_rows {
        a.append_value(i as i32);
        c.append_value(i as i32);
        d.append_value((i * 1000000) as i128);
        if allow_nulls && i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("this is string number {i}"));
        }
    }
    let a = a.finish();
    let b = b.finish();
    let c = c.finish();
    let d = d.finish();
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(d)],
    ).unwrap()
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);