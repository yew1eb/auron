#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Usage: ./run.sh
# Builds necessary JARs, generates data and queries, and runs fuzz tests for Auron Spark.
# Environment variables:
#   SPARK_HOME - path to Spark installation
#   SPARK_MASTER - Spark master URL (default: local[*])
#   SCALA_MAJOR_VERSION - Scala major version to use (default: 2.12)
#   SPARK_MAJOR_VERSION - Spark major version to use (default: 3.5)
#   NUM_FILES - number of data files to generate (default: 2)
#   NUM_ROWS - number of rows per file (default: 200)
#   NUM_QUERIES - number of queries to generate (default: 500)

set -eux

DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_MASTER=local
SPARK_HOME=/Users/yew1eb/workspaces/spark-3.5.7-bin-hadoop3
AURON_FUZZ_JAR="${DIR}/target/auron-fuzz-testing-1.0-SNAPSHOT-jar-with-dependencies.jar"
NUM_FILES="${NUM_FILES:-2}"
NUM_ROWS="${NUM_ROWS:-200}"
NUM_QUERIES="${NUM_QUERIES:-500}"

echo "Generating data..."
"${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER}" \
  --class org.apache.auron.fuzz.Main \
  "${AURON_FUZZ_JAR}" \
  data --num-files="${NUM_FILES}" --num-rows="${NUM_ROWS}" \
  --exclude-negative-zero \
  --generate-arrays --generate-structs --generate-maps

echo "Generating queries..."
"${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER}" \
  --class org.apache.auron.fuzz.Main \
  "${AURON_FUZZ_JAR}" \
  queries --num-files="${NUM_FILES}" --num-queries="${NUM_QUERIES}"


echo "Running fuzz tests..."

"${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER}" \
  --conf spark.memory.offHeap.enabled=false \
  --conf spark.memory.offHeap.size=4g \
  --conf spark.sql.extensions=org.apache.spark.sql.auron.AuronSparkSessionExtension \
  --conf spark.auron.enable=true \
  --conf spark.shuffle.manager=org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager \
  --class org.apache.auron.fuzz.Main \
  "${AURON_FUZZ_JAR}" \
  run --native-engine=auron --num-files="${NUM_FILES}" --filename="queries.sql"
