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

use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};


use arrow::{
    array::{Array, BooleanArray, StringArray},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
    physical_expr::PhysicalExprRef,
    physical_plan::PhysicalExpr,
};
use datafusion_ext_commons::df_execution_err;
use datafusion_ext_exprs::string_starts_with::StringStartsWithExpr;
use datafusion::physical_expr::{expressions as phys_expr};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

fn random_string(len: usize, rng: &mut StdRng) -> String {
    (0..len)
        .map(|_| {
            let idx = rng.gen_range(0..CHARS.len());
            CHARS[idx] as char
        })
        .collect()
}

fn make_realistic_batch(batch_size: usize, typical_prefix: &str, rng: &mut StdRng) -> ArrayRef {
    let mut data = Vec::with_capacity(batch_size);
    for i in 0..batch_size {
        if i % 20 == 0 {
            data.push(None);
        } else if i % 3 == 0 {
            data.push(Some(format!("{typical_prefix}{}", random_string(16, rng))));
        } else if i % 5 == 0 {
            data.push(Some(typical_prefix.to_string()));
        } else {
            let rand_len = rng.gen_range(8..64);
            data.push(Some(random_string(rand_len, rng)));
        }
    }
    Arc::new(StringArray::from(data))
}

fn string_starts_with_expr_benchmark(c: &mut Criterion) {
    let typical_prefixes = &["ab", "user_", "2023-", "prod", "123test"];
    let batch_sizes = &[1024, 4096, 1638400];
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)]));

    let mut group = c.benchmark_group("Auron StringStartsWithExpr");

    for prefix in typical_prefixes {
        for &batch_size in batch_sizes {
            let mut rng = StdRng::from_seed([batch_size as u8; 32]);
            let array = make_realistic_batch(batch_size, prefix, &mut rng);
            let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

            let input_expr = phys_expr::lit("rarrr");
            let expr = Arc::new(StringStartsWithExpr::new(input_expr, prefix.to_string()));

            group.bench_with_input(
                BenchmarkId::from_parameter(format!("prefix={}-batch={}", prefix, batch_size)),
                &(expr, &batch),
                |b, (expr, batch)| {
                    b.iter(|| {
                        let _ret = expr.evaluate(batch).unwrap().into_array(batch.num_rows()).unwrap();
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, string_starts_with_expr_benchmark);
criterion_main!(benches);