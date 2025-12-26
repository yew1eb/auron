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
package org.apache.spark.sql.hive.auron.paimon

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class PaimonHiveTableSuite extends QueryTest with SharedSparkSession {
  protected val rootPath: String = getClass.getResource("/").getPath

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension,org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.auron.enable", "true")
      .set("spark.ui.enabled", "true")
      .set("spark.auron.enable.paimon.scan", "true")
      .set("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.paimon.warehouse", s"file://$rootPath/data-paimon")
      .set("spark.sql.sources.useV1SourceList", "hive")
  }

  test("paimon Scan") {
    Seq("parquet", "orc").foreach { format =>
      withTable(s"paimon_${format}_tbl") {
        sql("USE paimon.default")
        sql(s"DROP TABLE IF EXISTS paimon_${format}_tbl")
        sql(
          s"CREATE TABLE paimon_${format}_tbl (id INT, name STRING) " +
            s"TBLPROPERTIES ('file.format'='$format')")
        sql(s"INSERT INTO paimon_${format}_tbl VALUES (1, 'Bob'), (2, 'Blue'), (3, 'Mike')")

        val df = spark.sql(s"""
                                |SELECT * FROM paimon_${format}_tbl;
                                |""".stripMargin)
        df.show()
        println(df.queryExecution.executedPlan)
        Thread.sleep(100000000)
      }
    }
  }

  test("paimon partitioned table") {
    withTable("paimon_tbl") {
      sql("DROP TABLE IF EXISTS paimon_tbl")
      sql(
        "CREATE TABLE paimon_tbl (id INT, p1 STRING, p2 STRING) USING paimon PARTITIONED BY (p1, p2)")
      sql("INSERT INTO paimon_tbl VALUES (1, '1', '1'), (2, '1', '2')")
      spark.sql("""
                           |SELECT p1 FROM paimon_tbl WHERE p1 = '1' ORDER BY id;
                           |""".stripMargin)
      spark.sql("""
                           |SELECT p2 FROM paimon_tbl WHERE p1 = '1' ORDER BY id;
                           |""".stripMargin)
      spark.sql("""
                           |SELECT p1 FROM paimon_tbl WHERE p2 = '1';
                           |""".stripMargin)
      spark.sql("""
                           |SELECT p2 FROM paimon_tbl WHERE p2 = '1';
                           |""".stripMargin)
      spark.sql("""
                           |SELECT id, p2 FROM paimon_tbl WHERE p1 = '1' and p2 = '2';
                           |""".stripMargin)
      spark.sql("""
                           |SELECT id FROM paimon_tbl ORDER BY id;
                           |""".stripMargin)
    }
  }

  test("paimon transformer exists with bucket table") {
    withTable(s"paimon_tbl") {
      spark.sql(s"""
             |CREATE TABLE paimon_tbl (id INT, name STRING)
             |USING paimon
             |TBLPROPERTIES (
             | 'bucket' = '1',
             | 'bucket-key' = 'id'
             |)
             |""".stripMargin)
      spark.sql(s"INSERT INTO paimon_tbl VALUES (1, 'Bob'), (2, 'Blue'), (3, 'Mike')")
      val df = spark.sql("SELECT * FROM paimon_tbl")
      df.show()
      println(df.queryExecution.executedPlan)
    }
  }
}
