#!/usr/bin/env bash

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

set -exo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AURON_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# mvn clean install -Pspark-3.5 -Pscala-2.12

JAR_PATH=$(find "$AURON_DIR/dev/auron-it/target/" -name "auron-it-*.jar" | head -n 1)

if [[ -z "${SPARK_HOME:-}" ]]; then
  echo "ERROR: SPARK_HOME must be set"
  exit 1
fi

if [[ ! -f "$JAR_PATH" ]]; then
  echo "ERROR: auron-it.jar not found"
  exit 1
fi

echo "=== Auron TPC-DS Integration Test (tpcds-it.sh) ==="
echo "SPARK_HOME: $SPARK_HOME"
echo "Runner JAR: $JAR_PATH"

# Split input arguments into two parts: Spark confs and args
SPARK_CONF=()
ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --master=*)
      SPARK_CONF+=("$1") ;;
    --conf)
      shift
      SPARK_CONF+=("--conf" "$1") ;;
    *)
      ARGS+=("$1") ;;
  esac
  shift
done

exec $SPARK_HOME/bin/spark-submit \
  --class org.apache.auron.integration.AuronTPCDSTestRunner \
  --driver-memory 5g \
  --conf spark.ui.enabled=false \
  --conf spark.sql.extensions=org.apache.spark.sql.auron.AuronSparkSessionExtension \
  --conf spark.shuffle.manager=org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager \
  --conf spark.sql.shuffle.partitions=1000 \
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=16777216 \
  --conf spark.sql.autoBroadcastJoinThreshold=1048576 \
  --conf spark.sql.broadcastTimeout=900s \
  --conf spark.driver.memoryOverhead=3072 \
  --conf spark.auron.memoryFraction=0.8 \
  "${SPARK_CONF[@]}" \
  "$JAR_PATH" \
  "${ARGS[@]}"