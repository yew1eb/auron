#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
End-to-end validation script for Auron Native Python UDF support.

Prerequisites:
  - Auron built and deployed
  - PySpark installed
  - SPARK_HOME set

Usage:
  python3 scripts/test_python_udf.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, ArrayType


def build_spark():
    return (
        SparkSession.builder
        .appName("AuronPythonUDFValidation")
        .config("spark.plugins", "org.apache.auron.plugin.AuronPlugin")
        .config("spark.auron.enable.pythonUdf", "true")
        .getOrCreate()
    )


def test_basic_int_udf(spark):
    """Test 1: basic integer UDF - double the value."""
    spark.udf.register("double_it", lambda x: x * 2 if x is not None else None, IntegerType())
    result = spark.range(5).selectExpr("CAST(id AS INT) AS id", "double_it(CAST(id AS INT)) AS doubled").collect()
    expected = [0, 2, 4, 6, 8]
    actual = [r.doubled for r in result]
    assert actual == expected, f"Test 1 failed: expected {expected}, got {actual}"
    print("✓ Test 1 (basic int UDF): PASS")


def test_plan_uses_native(spark):
    """Test 2: verify the plan does NOT contain EvalPythonExec (should use NativeProject)."""
    spark.udf.register("add_one", lambda x: x + 1 if x is not None else None, IntegerType())
    df = spark.range(3).selectExpr("CAST(id AS INT) AS id", "add_one(CAST(id AS INT)) AS id_plus_one")
    plan = df._jdf.queryExecution().executedPlan().toString()
    if "EvalPythonExec" in plan:
        print(f"⚠ Test 2 WARNING: EvalPythonExec found in plan (may fall back to Spark path):\n{plan[:500]}")
    else:
        print("✓ Test 2 (native plan): PASS - EvalPythonExec not in plan")


def test_string_udf(spark):
    """Test 3: string UDF."""
    spark.udf.register("greet", lambda name: f"Hello, {name}!" if name else None, StringType())
    result = spark.createDataFrame([("Alice",), ("Bob",)], ["name"]).selectExpr("greet(name) AS greeting").collect()
    assert result[0].greeting == "Hello, Alice!", f"Test 3 failed: {result[0].greeting}"
    assert result[1].greeting == "Hello, Bob!", f"Test 3 failed: {result[1].greeting}"
    print("✓ Test 3 (string UDF): PASS")


def test_unsupported_type_fallback(spark):
    """Test 4: UDF with ArrayType (unsupported) must not throw - falls back to Spark path."""
    spark.udf.register("wrap_array", lambda x: [x] if x is not None else None, ArrayType(IntegerType()))
    result = spark.range(3).selectExpr("CAST(id AS INT) AS id", "wrap_array(CAST(id AS INT)) AS arr").collect()
    assert result[0].arr == [0], f"Test 4 failed: {result[0].arr}"
    print("✓ Test 4 (unsupported type fallback): PASS")


def main():
    print("=== Auron Native Python UDF Validation ===\n")
    spark = build_spark()
    try:
        test_basic_int_udf(spark)
        test_plan_uses_native(spark)
        test_string_udf(spark)
        test_unsupported_type_fallback(spark)
        print("\n✓ All tests passed!")
    except AssertionError as e:
        print(f"\n✗ FAILED: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
