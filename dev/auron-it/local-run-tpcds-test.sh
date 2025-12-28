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

#   --data-location dev/tpcds_1g \
#   --query-filter q1,q2,a3 \
#   --plan-check
$SCRIPT_DIR/auron-it.sh \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  --conf spark.celeborn.client.spark.shuffle.writer=hash \
    --type tpcds \
    --data-location /Users/yew1eb/workspaces/tpcds-validator/tpcds_1g