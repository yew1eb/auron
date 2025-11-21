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

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.io.Source

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DoubleType

abstract class AuronTPCHSuite extends QueryTest with SharedSparkSession {
  protected val rootPath: String = getClass.getResource("/").getPath
  protected val tpchDataPath: String = rootPath + "/tpch-data-parquet"
  protected val tpchQueriesPath: String = rootPath + "/tpch-queries"
  protected val tpchResultsPath: String = rootPath + "/tpch-query-results"
  protected val tpchPlanPath: String = rootPath + "/tpch-plan-stability"

  val tpchQueries: Seq[String] = Seq(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21",
    "q22")

  def shouldCheck(): Boolean = {
    Shims.get.shimVersion match {
      case "spark-3.5" => true
      case _ => false
    }
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.auron.enable", "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
    createTPCHTables()
  }

  protected def createTPCHTables(): Unit = {
    Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
      .foreach { tableName =>
        spark.read.parquet(s"$tpchDataPath/$tableName").createOrReplaceTempView(tableName)
        tableName -> spark.table(tableName).count()
      }
  }

  protected def compareResultStr(
      sqlNum: String,
      result: Array[Row],
      resultsPath: String): Unit = {
    val resultStr = new StringBuffer()
    resultStr.append(result.length).append("\n")
    result.foreach(r => resultStr.append(r.mkString("|-|")).append("\n"))
    val expectedResult =
      Source.fromFile(new File(resultsPath + "/" + sqlNum + ".out"), "UTF-8").mkString

    if (expectedResult != resultStr.toString) {
      fail(s"""
         |=== $sqlNum result does NOT match expected ===
         |[Expected]
         |${expectedResult}
         |[Actual]
         |${resultStr.toString}
         |""".stripMargin)
    }
  }

  protected def compareDoubleResult(
      sqlNum: String,
      result: Array[Row],
      resultsPath: String): Unit = {
    val expectedResult =
      Source.fromFile(new File(resultsPath + "/" + sqlNum + ".out"), "UTF-8").getLines()
    var recordCnt = expectedResult.next().toInt
    assert(result.size == recordCnt)
    for (row <- result) {
      assert(expectedResult.hasNext)
      val expectedRow = expectedResult.next().split("\\|-\\|")
      var i = 0
      row.schema.foreach(s => {
        s match {
          case d if d.dataType == DoubleType =>
            val v1 = row.getDouble(i)
            val v2 = expectedRow(i).toDouble
            assert(
              Math.abs(v1 - v2) < 0.00001,
              s"""
                 |=== row $i - $sqlNum result does NOT match expected ===
                 |[Expected]
                 |${expectedResult}
                 |[Actual]
                 |${result.toString}
                 |""".stripMargin)
          case _ =>
            val v1 = row.get(i).toString
            val v2 = result(i)
            assert(
              v1.equals(v2),
              s"""
                 |=== row $i - $sqlNum result does NOT match expected ===
                 |[Expected]
                 |${expectedResult}
                 |[Actual]
                 |${result.toString}
                 |""".stripMargin)
        }
        i += 1
      })
    }
  }

  private val normalizeRegex = "#\\d+L?".r
  private val planIdRegex = "plan_id=\\d+".r

  private def normalizeLocation(plan: String): String = {
    plan.replaceAll("""file:/[^,\s\]\)]+""", "file:/<warehouse_dir>")
  }

  private def normalizeFormattedPlan(plan: String): String = {

    val map = new mutable.HashMap[String, String]()
    normalizeRegex
      .findAllMatchIn(plan)
      .map(_.toString)
      .foreach(map.getOrElseUpdate(_, (map.size + 1).toString))
    val exprIdNormalized =
      normalizeRegex.replaceAllIn(plan, regexMatch => s"#${map(regexMatch.toString)}")

    // Normalize the plan id in Exchange nodes. See `Exchange.stringArgs`.
    val planIdMap = new mutable.HashMap[String, String]()
    planIdRegex
      .findAllMatchIn(exprIdNormalized)
      .map(_.toString)
      .foreach(planIdMap.getOrElseUpdate(_, (planIdMap.size + 1).toString))
    val planIdNormalized =
      planIdRegex.replaceAllIn(
        exprIdNormalized,
        regexMatch => s"plan_id=${planIdMap(regexMatch.toString)}")

    // Spark QueryStageExec will take its id as argument, replace it with X
    val argumentsNormalized = planIdNormalized
      .replaceAll("Arguments: [0-9]+, [0-9]+", "Arguments: X, X")
      .replaceAll("Arguments: [0-9]+", "Arguments: X")

    normalizeLocation(argumentsNormalized)
  }

  protected def verifyResult(df: DataFrame, sqlNum: String, resultsPath: String): Unit = {
    val result = df.collect()
    if (df.schema.exists(_.dataType == DoubleType)) {
      compareDoubleResult(sqlNum, result, resultsPath)
    } else {
      compareResultStr(sqlNum, result, resultsPath)
    }
  }

  protected def verifyPlan(df: DataFrame, sqlNum: String, planPath: String): Unit = {
    val expectedFile = new File(planPath, s"$sqlNum.txt")
    val expected = FileUtils.readFileToString(expectedFile, StandardCharsets.UTF_8)
    val actual = normalizeFormattedPlan(df.queryExecution.explainString(FormattedMode))
    val actualFile = new File(FileUtils.getTempDirectory, s"actual.$sqlNum.txt")
    FileUtils.writeStringToFile(actualFile, actual, StandardCharsets.UTF_8)

    if (false) {
      FileUtils.writeStringToFile(expectedFile, actual, StandardCharsets.UTF_8)
      return
    }

    if (expected != actual) {
      fail(s"""
           |Plans did not match:: $sqlNum
           |approved explain plan: ${expectedFile.getAbsolutePath}
           |
           |$expected
           |actual explain plan: ${actualFile.getAbsolutePath}
           |
           |$actual
           |""".stripMargin)
    }
  }

  if (shouldCheck()) {
    tpchQueries.foreach { sqlNum =>
      //Seq("q17").foreach { sqlNum =>
      test("TPC-H " + sqlNum) {
        val sqlFile = tpchQueriesPath + "/" + sqlNum + ".sql"
        val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8").mkString
        val df = spark.sql(sqlStr)
        verifyResult(df, sqlNum, tpchResultsPath)
        verifyPlan(df, sqlNum, tpchPlanPath)
      }
    }
  }
}

class AuronTPCHV1Suite extends AuronTPCHSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
  }
}
