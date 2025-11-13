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
package org.apache.spark.sql.auron

import org.apache.auron.jni.{AuronAdaptor, SparkAuronAdaptor}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.execution.auron.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.test.SharedSparkSession

import java.io.File

class AuronCheckConvertShuffleExchangeSuite
  extends QueryTest
    with SharedSparkSession
    with AuronSQLTestHelper
    with org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper {

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val basePath: String = rootPath + "unit-tests-working-home"

  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

  def prepareWorkDir(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
  }

  override protected def beforeAll(): Unit = {
    prepareWorkDir()
    AuronAdaptor.initInstance(new SparkAuronAdaptor)
  }

  test(
    "test set auron shuffle manager convert to native shuffle exchange where set spark.auron.enable is true") {
      val customSpark = SparkSession
        .builder()
        .master("local[2]")
        .appName("checkConvertToNativeShuffleManger")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.memory.offHeap.enabled", "false")
        .config("spark.auron.enable", "true")
        .getOrCreate()

      customSpark.sql("drop table if exists test_shuffle")
      customSpark.sql(
        "create table if not exists test_shuffle using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val df =
        customSpark.sql("select c1, count(1) from test_shuffle group by c1")

      val shuffleExchangeExec = collectFirst(df.queryExecution.executedPlan) {
        case shuffleExchangeExec: ShuffleExchangeExec =>
            shuffleExchangeExec
      }
      val afterConvertPlan = AuronConverters.convertSparkPlan(shuffleExchangeExec.get)
      assert(afterConvertPlan.isInstanceOf[NativeShuffleExchangeExec])
      checkAnswer(df, Seq(Row(1, 1)))
  }

  test(
    "test set non auron shuffle manager do not convert to native shuffle exchange where set spark.auron.enable is true") {
      val customSpark = SparkSession
        .builder()
        .master("local[2]")
        .appName("checkConvertToNativeShuffleManger")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
        .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.memory.offHeap.enabled", "false")
        .config("spark.auron.enable", "true")
        .getOrCreate()

      customSpark.sql("drop table if exists test_shuffle")
      customSpark.sql(
        "create table if not exists test_shuffle using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val df =
        customSpark.sql("select c1, count(1) from test_shuffle group by c1")

      val shuffleExchangeExec = collectFirst(df.queryExecution.executedPlan) {
        case shuffleExchangeExec: ShuffleExchangeExec =>
          shuffleExchangeExec
      }
    val afterConvertPlan = AuronConverters.convertSparkPlan(shuffleExchangeExec.get)
    assert(afterConvertPlan.isInstanceOf[ShuffleExchangeExec])
    println(df.collect().mkString("Array(", ", ", ")"))
    Thread.sleep(10000000)
    checkAnswer(df, Seq(Row(1, 1)))
  }
}
