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
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};
use arrow::{
    array::{ArrayRef, new_empty_array},
    datatypes::{DataType, Schema},
};
use datafusion::{
    error::Result,
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExprRef, physical_exprs_bag_equal},
    physical_expr_common::physical_expr::DynEq,
    physical_plan::PhysicalExpr,
};
use arrow::record_batch::RecordBatch;
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use crate::type_convert::{arrow_scalar_to_py, build_arrow_array, py_to_arrow_scalar};

pub struct SparkPythonUDFWrapperExpr {
    pub func_bytes: Vec<u8>,
    pub return_type: DataType,
    pub return_nullable: bool,
    pub params: Vec<PhysicalExprRef>,
    py_func: OnceCell<PyObject>,
    expr_string: String,
}

impl SparkPythonUDFWrapperExpr {
    pub fn try_new(
        func_bytes: Vec<u8>,
        return_type: DataType,
        return_nullable: bool,
        params: Vec<PhysicalExprRef>,
        expr_string: String,
    ) -> Result<Self> {
        Ok(Self {
            func_bytes,
            return_type,
            return_nullable,
            params,
            py_func: OnceCell::new(),
            expr_string,
        })
    }
}

impl PartialEq for SparkPythonUDFWrapperExpr {
    fn eq(&self, other: &Self) -> bool {
        physical_exprs_bag_equal(&self.params, &other.params)
            && self.func_bytes == other.func_bytes
            && self.return_type == other.return_type
            && self.return_nullable == other.return_nullable
    }
}

impl DynEq for SparkPythonUDFWrapperExpr {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|o| o.eq(self))
            .unwrap_or(false)
    }
}

impl Display for SparkPythonUDFWrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PythonUDF({})", self.expr_string)
    }
}

impl Debug for SparkPythonUDFWrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PythonUDF({})", self.expr_string)
    }
}

impl Hash for SparkPythonUDFWrapperExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.func_bytes.hash(state);
    }
}

impl PhysicalExpr for SparkPythonUDFWrapperExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.return_nullable)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(&self.return_type)));
        }

        // Evaluate all parameter expressions against the batch
        let params: Vec<ArrayRef> = self
            .params
            .iter()
            .map(|p| p.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;

        let func_bytes = &self.func_bytes;
        let return_type = &self.return_type;
        let py_func = &self.py_func;

        let execute_python = || -> Result<ArrayRef> {
            Python::with_gil(|py| {
                // Load and cache the Python callable (only once per executor)
                let callable = py_func
                    .get_or_try_init(|| -> PyResult<PyObject> {
                        let cloudpickle = py.import("cloudpickle")?;
                        let func = cloudpickle.call_method1("loads", (func_bytes.as_slice(),))?;
                        Ok(func.unbind())
                    })
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                let mut results = Vec::with_capacity(num_rows);
                for i in 0..num_rows {
                    let py_args: Vec<PyObject> = params
                        .iter()
                        .map(|col| arrow_scalar_to_py(py, col, i))
                        .collect::<PyResult<_>>()
                        .map_err(|e| {
                            datafusion::error::DataFusionError::External(Box::new(e))
                        })?;
                    let py_tuple = PyTuple::new(py, py_args)
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                    let py_result = callable
                        .call1(py, py_tuple)
                        .map_err(|e| {
                            datafusion::error::DataFusionError::External(Box::new(e))
                        })?;
                    results.push(py_to_arrow_scalar(py, &py_result, return_type)?);
                }
                build_arrow_array(results, return_type)
            })
        };

        // Use block_in_place if in a tokio multi-thread context; otherwise call directly.
        // This prevents blocking the tokio worker thread while holding the GIL.
        let result = if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(execute_python)?
        } else {
            execute_python()?
        };

        Ok(ColumnarValue::Array(result))
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        self.params.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self::try_new(
            self.func_bytes.clone(),
            self.return_type.clone(),
            self.return_nullable,
            children,
            self.expr_string.clone(),
        )?))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;
    use pyo3::Python;
    use std::sync::Arc;

    fn make_batch(values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[test]
    fn test_double_int_udf() {
        pyo3::prepare_freethreaded_python();
        let func_bytes = Python::with_gil(|py| {
            let cloudpickle = py.import("cloudpickle").unwrap();
            #[allow(deprecated)]
            let code = py
                .eval_bound("lambda x: None if x is None else x * 2", None, None)
                .unwrap();
            cloudpickle
                .call_method1("dumps", (code,))
                .unwrap()
                .extract::<Vec<u8>>()
                .unwrap()
        });
        let expr = SparkPythonUDFWrapperExpr::try_new(
            func_bytes,
            DataType::Int32,
            true,
            vec![Arc::new(Column::new("x", 0))],
            "lambda x: x * 2".to_string(),
        )
        .unwrap();
        let batch = make_batch(vec![Some(3), Some(5), None]);
        let result = expr.evaluate(&batch).unwrap().into_array(3).unwrap();
        let ints = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.value(0), 6);
        assert_eq!(ints.value(1), 10);
        assert!(ints.is_null(2));
    }

    #[test]
    fn test_empty_batch() {
        pyo3::prepare_freethreaded_python();
        let func_bytes = Python::with_gil(|py| {
            let cloudpickle = py.import("cloudpickle").unwrap();
            #[allow(deprecated)]
            let code = py.eval_bound("lambda x: x", None, None).unwrap();
            cloudpickle
                .call_method1("dumps", (code,))
                .unwrap()
                .extract::<Vec<u8>>()
                .unwrap()
        });
        let expr = SparkPythonUDFWrapperExpr::try_new(
            func_bytes,
            DataType::Int32,
            true,
            vec![Arc::new(Column::new("x", 0))],
            "identity".to_string(),
        )
        .unwrap();
        let batch = make_batch(vec![]);
        let result = expr.evaluate(&batch).unwrap().into_array(0).unwrap();
        assert_eq!(result.len(), 0);
    }
}
