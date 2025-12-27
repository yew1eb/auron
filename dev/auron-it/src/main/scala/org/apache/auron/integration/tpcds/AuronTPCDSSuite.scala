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
package org.apache.auron.integration.runner

import org.apache.auron.integration.Suite
import org.apache.auron.integration.cli.ArgsParser
import org.apache.auron.integration.exec.SparkFactory
import org.apache.auron.integration.tpcds.TPCDSQueriesCompare
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.types.{DoubleType, StructType}

import java.io.File
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.util.Try
import scala.util.matching.Regex

object AuronTPCDSSuite extends Suite {
  protected val regenGoldenFiles: Boolean =
    sys.env.getOrElse("REGEN_TPCDS_GOLDEN_FILES", "0") == "1"

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val tpcdsQueriesPath: String = rootPath + "/tpcds-queries"
  protected val tpcdsResultsPath: String = rootPath + "/tpcds-query-results"
  protected val tpcdsPlanPath: String = rootPath + "/tpcds-plan-stability"

  protected val colSep: String = "<|COL|>"

  val tpcdsQueries: Seq[String] = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
    "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
    "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
    "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
    "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
    "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
    "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
    "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
    "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
    "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

  val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    "time_dim", "web_page")

  def setupTables(dataLocation: String): Map[String, Long] =
    tables.map { tableName =>
        val tablePath = new File(dataLocation, tableName).getAbsolutePath
        val tableDF = spark.read.format("parquet").load(tablePath)
        tableDF.createOrReplaceTempView(tableName)
      tableName -> spark.table(tableName).count()
      }.toMap


  val baseConf = new SparkConf()
    .setAppName("validate-tpcds-queries")
    .set("spark.sql.parquet.compression.codec", "snappy")
    .set("spark.sql.shuffle.partitions", "4")
    .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
    // Since Spark 3.0, `spark.sql.crossJoin.enabled` is true by default
    .set("spark.sql.crossJoin.enabled", "true")
    .set("spark.sql.sources.useV1SourceList", "parquet")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")

  val spark = SparkSession.builder.config(baseConf).getOrCreate()

  override def run(args: ArgsParser.Config): Int = {

    val filteredQueries = filterQueries(tpcdsQueries, args.queryFilter)

    val tableSizes = setupTables(args.dataLocation)
    println(tableSizes)

    TPCDSQueriesCompare.runQueries(session = spark, queryLocation = tpcdsQueriesPath, queries = filteredQueries)
  }


  private def filterQueries(
                             origQueries: Seq[String],
                             queryFilter: Seq[String],
                             nameSuffix: String = ""): Seq[String] = {
    if (queryFilter.nonEmpty) {
      if (nameSuffix.nonEmpty) {
        origQueries.filter { name => queryFilter.contains(s"$name$nameSuffix") }
      } else {
        origQueries.filter(queryFilter.contains)
      }
    } else {
      origQueries
    }
  }

  override def name: String = ???

  override def prepareSessions(): Unit = ???

  override def registerTables(): Unit = ???

  override def loadQueries(): Seq[String] = ???

}