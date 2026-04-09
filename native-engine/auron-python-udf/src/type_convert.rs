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

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeStringArray, StringArray,
};
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion_ext_commons::df_execution_err;
use pyo3::prelude::*;

/// Convert a single Arrow array element at row_idx to a Python object.
/// Returns py.None() for null values.
pub fn arrow_scalar_to_py(py: Python<'_>, array: &ArrayRef, row_idx: usize) -> PyResult<PyObject> {
    if array.is_null(row_idx) {
        return Ok(py.None());
    }
    match array.data_type() {
        DataType::Int32 => {
            let v = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row_idx);
            Ok(v.into_pyobject(py).unwrap().into_any().unbind())
        }
        DataType::Int64 => {
            let v = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row_idx);
            Ok(v.into_pyobject(py).unwrap().into_any().unbind())
        }
        DataType::Float32 => {
            // Python float is always f64
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row_idx) as f64;
            Ok(v.into_pyobject(py).unwrap().into_any().unbind())
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row_idx);
            Ok(v.into_pyobject(py).unwrap().into_any().unbind())
        }
        DataType::Utf8 => {
            let v = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row_idx);
            Ok(v.into_pyobject(py).unwrap().into_any().unbind())
        }
        DataType::LargeUtf8 => {
            let v = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row_idx);
            Ok(v.into_pyobject(py).unwrap().into_any().unbind())
        }
        DataType::Boolean => {
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row_idx);
            Ok(v.into_pyobject(py).unwrap().to_owned().into_any().unbind())
        }
        dt => Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "Unsupported Arrow type for Python UDF parameter: {dt}"
        ))),
    }
}

/// Convert a Python object to an Arrow ScalarValue of the given DataType.
/// Returns ScalarValue::T(None) for Python None.
pub fn py_to_arrow_scalar(
    py: Python<'_>,
    obj: &PyObject,
    data_type: &DataType,
) -> Result<ScalarValue> {
    if obj.is_none(py) {
        return Ok(match data_type {
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Utf8 => ScalarValue::Utf8(None),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            DataType::Boolean => ScalarValue::Boolean(None),
            dt => df_execution_err!("Unsupported return type for Python UDF: {dt}")?,
        });
    }
    match data_type {
        DataType::Int32 => {
            let v = obj
                .bind(py)
                .extract::<i32>()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            Ok(ScalarValue::Int32(Some(v)))
        }
        DataType::Int64 => {
            let v = obj
                .bind(py)
                .extract::<i64>()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            Ok(ScalarValue::Int64(Some(v)))
        }
        DataType::Float32 => {
            let v = obj
                .bind(py)
                .extract::<f32>()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            Ok(ScalarValue::Float32(Some(v)))
        }
        DataType::Float64 => {
            let v = obj
                .bind(py)
                .extract::<f64>()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            Ok(ScalarValue::Float64(Some(v)))
        }
        DataType::Utf8 => {
            let v = obj
                .bind(py)
                .extract::<String>()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            Ok(ScalarValue::Utf8(Some(v)))
        }
        DataType::LargeUtf8 => {
            let v = obj
                .bind(py)
                .extract::<String>()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            Ok(ScalarValue::LargeUtf8(Some(v)))
        }
        DataType::Boolean => {
            let v = obj
                .bind(py)
                .extract::<bool>()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            Ok(ScalarValue::Boolean(Some(v)))
        }
        dt => df_execution_err!("Unsupported return type for Python UDF: {dt}"),
    }
}

/// Build an Arrow ArrayRef from a Vec of ScalarValues.
pub fn build_arrow_array(scalars: Vec<ScalarValue>, _data_type: &DataType) -> Result<ArrayRef> {
    ScalarValue::iter_to_array(scalars.into_iter())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use pyo3::Python;
    use std::sync::Arc;

    fn init() {
        pyo3::prepare_freethreaded_python();
    }

    #[test]
    fn test_int32_roundtrip() {
        init();
        Python::with_gil(|py| {
            let arr = Arc::new(Int32Array::from(vec![Some(42)])) as ArrayRef;
            let py_val = arrow_scalar_to_py(py, &arr, 0).unwrap();
            let result = py_to_arrow_scalar(py, &py_val, &DataType::Int32).unwrap();
            assert_eq!(result, ScalarValue::Int32(Some(42)));
        });
    }

    #[test]
    fn test_null_int32() {
        init();
        Python::with_gil(|py| {
            let arr = Arc::new(Int32Array::from(vec![None::<i32>])) as ArrayRef;
            let py_val = arrow_scalar_to_py(py, &arr, 0).unwrap();
            let result = py_to_arrow_scalar(py, &py_val, &DataType::Int32).unwrap();
            assert_eq!(result, ScalarValue::Int32(None));
        });
    }

    #[test]
    fn test_int64_roundtrip() {
        init();
        Python::with_gil(|py| {
            let arr = Arc::new(Int64Array::from(vec![Some(100i64)])) as ArrayRef;
            let py_val = arrow_scalar_to_py(py, &arr, 0).unwrap();
            let result = py_to_arrow_scalar(py, &py_val, &DataType::Int64).unwrap();
            assert_eq!(result, ScalarValue::Int64(Some(100)));
        });
    }

    #[test]
    fn test_float64_roundtrip() {
        init();
        Python::with_gil(|py| {
            let arr = Arc::new(Float64Array::from(vec![Some(3.14f64)])) as ArrayRef;
            let py_val = arrow_scalar_to_py(py, &arr, 0).unwrap();
            let result = py_to_arrow_scalar(py, &py_val, &DataType::Float64).unwrap();
            assert_eq!(result, ScalarValue::Float64(Some(3.14)));
        });
    }

    #[test]
    fn test_string_roundtrip() {
        init();
        Python::with_gil(|py| {
            let arr = Arc::new(StringArray::from(vec![Some("hello")])) as ArrayRef;
            let py_val = arrow_scalar_to_py(py, &arr, 0).unwrap();
            let result = py_to_arrow_scalar(py, &py_val, &DataType::Utf8).unwrap();
            assert_eq!(result, ScalarValue::Utf8(Some("hello".to_string())));
        });
    }

    #[test]
    fn test_bool_roundtrip() {
        init();
        Python::with_gil(|py| {
            let arr = Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef;
            let py_val = arrow_scalar_to_py(py, &arr, 0).unwrap();
            let result = py_to_arrow_scalar(py, &py_val, &DataType::Boolean).unwrap();
            assert_eq!(result, ScalarValue::Boolean(Some(true)));
        });
    }

    #[test]
    fn test_build_array() {
        init();
        let scalars = vec![
            ScalarValue::Int32(Some(1)),
            ScalarValue::Int32(None),
            ScalarValue::Int32(Some(3)),
        ];
        let arr = build_arrow_array(scalars, &DataType::Int32).unwrap();
        let ints = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.value(0), 1);
        assert!(ints.is_null(1));
        assert_eq!(ints.value(2), 3);
    }
}
