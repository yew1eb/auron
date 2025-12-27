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
set -ex

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AURON_DIR="$SCRIPT_DIR/../.."

run_all_sparkverion() {
  local spark_versions=("3.0" "3.1" "3.2" "3.3" "3.4" "3.5")
  local scala_version="2.12"
  local hadoop_version="hadoop3"

  for spark_ver in "${spark_versions[@]}"; do
    echo "=================================================="
    echo "Starting build and test for Spark version: $spark_ver (Scala $scala_version), $hadoop_version"
    echo "=================================================="

    cd $AURON_DIR
    ./auron-build.sh --pre --sparkver "$spark_ver" --scalaver "$scala_version" --skiptests true

    cd $SCRIPT_DIR
    ../../build/mvn -Ppre -Pspark-"$spark_ver" -Pscala-"$scala_version" -Pjdk-8 -P"$hadoop_version" \
        -Dsuites=org.apache.auron.integration.AuronTPCDSV1Suite test
  done
}

run_all_sparkverion

run_with_celeborn() {
  # ./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --skiptests true --celeborn 0.5

  export SPARK_TPCDS_EXTRA_CONF="--conf spark.ui.enabled=true --conf spark.auron.ui.enabled=true --conf spark.shuffle.manager=org.apache.spark.sql.execution.auron.shuffle.celeborn.AuronCelebornShuffleManager --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.celeborn.client.spark.shuffle.writer=hash --conf spark.celeborn.client.push.replicate.enabled=false"
  #export SPARK_TPCDS_DATA=/home/runner/work/auron/auron/dev/tpcds_1g
  export SPARK_TPCDS_QUERY="q1,q2,q3,q4,q5,q6,q7,q8,q9"

  ../../build/mvn -Ppre -Pspark-3.5 -Pscala-2.12 -Pjdk-8 -Phadoop3  -Pceleborn,celeborn-0.5 \
    -Dsuites=org.apache.auron.integration.AuronTPCDSV1Suite test
}