/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron.expression

import org.apache.spark.SparkException
import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastExchangeExec

import org.apache.auron.BaseAuronSQLSuite

class AuronNativeSuite extends AuronQueryTest with BaseAuronSQLSuite {

  // FIXME
  // TODO
  // https://github.com/apache/datafusion-comet/pull/1693/files
  // Set/cancel with job tag on spark 3.5+
  test("test maxBroadcastTableSize") {
    withSQLConf("spark.sql.maxBroadcastTableSize" -> "10B") {
      spark.range(0, 1000).createOrReplaceTempView("t1")
      spark.range(0, 100).createOrReplaceTempView("t2")
      val df = spark.sql("select /*+ BROADCAST(t2) */ * from t1 join t2 on t1.id = t2.id")
      val exception = intercept[SparkException] {
        df.collect()
      }
      assert(
        exception.getMessage.contains("Cannot broadcast the table that is larger than 10.0 B"))
      val broadcasts = collect(df.queryExecution.executedPlan) {
        case p: NativeBroadcastExchangeExec => p
      }
      assert(broadcasts.size == 1)
    }
  }
}
