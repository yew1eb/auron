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
package org.apache.auron

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{AuronQueryTest, DataFrame, Row}
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DoubleType

abstract class AuronTPCHSuite extends AuronQueryTest with SharedSparkSession {

  protected val RE_GENERATE: Boolean = (sys.env.getOrElse("SPARK_TPCH_REGENERATE", "0") == "1")

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val tpchDataPath: String = rootPath + "/tpch-data-parquet"
  protected val tpchQueriesPath: String = rootPath + "/tpch-queries"
  protected val tpchResultsPath: String = rootPath + "/tpch-query-results"
  protected val tpchPlanPath: String = rootPath + "/tpch-plan-stability"

  protected val colSep: String = "<|COL|>"

  protected val tpchQueries: Seq[String] = (1 to 22).map("q" + _)

  protected val tpchTables: Seq[String] =
    Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

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
    tpchTables
      .foreach { tableName =>
        spark.read.parquet(s"$tpchDataPath/$tableName").createOrReplaceTempView(tableName)
        tableName -> spark.table(tableName).count()
      }
  }

  def shouldVerifyPlan(): Boolean = {
    Shims.get.shimVersion match {
      case "spark-3.5" => true
      case _ => false // TODO: Support for other Spark versions in the future
    }
  }

  protected def verifyResult(df: DataFrame, sqlNum: String, resultsPath: String): Unit = {
    val result = df.collect()
    if (df.schema.exists(_.dataType == DoubleType)) {
      compareDoubleResult(sqlNum, result, resultsPath)
    } else {
      compareResultStr(sqlNum, result, resultsPath)
    }
  }

  protected def compareResultStr(
      sqlNum: String,
      result: Array[Row],
      resultsPath: String): Unit = {
    val resultStr = new StringBuffer()
    resultStr.append(result.length).append("\n")
    result.foreach(r => resultStr.append(r.mkString(colSep)).append("\n"))

    if (RE_GENERATE) {
      FileUtils.writeStringToFile(
        new File(resultsPath + "/" + sqlNum + ".out"),
        resultStr.toString,
        StandardCharsets.UTF_8)
      return
    }

    val expectedResult =
      FileUtils.readFileToString(
        new File(resultsPath + "/" + sqlNum + ".out"),
        StandardCharsets.UTF_8)

    if (false) {
      FileUtils.writeStringToFile(
        new File(resultsPath + "/" + sqlNum + ".out"),
        resultStr.toString)
      return
    }

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

    if (RE_GENERATE) {
      var resultStr = new StringBuilder()
      resultStr.append(result.length + "\n")
      result.foreach { row => resultStr.append(row.mkString(colSep)).append("\n") }
      FileUtils.writeStringToFile(
        new File(resultsPath + "/" + sqlNum + ".out"),
        resultStr.toString,
        StandardCharsets.UTF_8)
      return
    }

    val expectedResult =
      FileUtils
        .readLines(new File(s"$resultsPath/$sqlNum.out"), StandardCharsets.UTF_8)
        .iterator()
    val expectedCount = expectedResult.next().toInt
    assert(result.length == expectedCount)

    result.zipWithIndex.foreach { case (row, rowIndex) =>
      assert(expectedResult.hasNext)
      val expectedRow = expectedResult.next().split(Regex.quote(colSep))

      row.schema.zipWithIndex.foreach { case (field, idx) =>
        field.dataType match {
          case DoubleType =>
            val actualValue = row.getDouble(idx)
            val expectedValue = expectedRow(idx).toDouble
            assert(
              Math.abs(actualValue - expectedValue) < 0.000001,
              failureMessage(sqlNum, rowIndex, expectedRow.mkString(colSep), row.toString))
          case _ =>
            val actualValue = row.get(idx).toString
            val expectedValue = expectedRow(idx)
            assert(
              actualValue.equals(expectedValue),
              failureMessage(sqlNum, rowIndex, expectedRow.mkString(colSep), row.toString))
        }
      }
    }
  }

  private def failureMessage(
      sqlNum: String,
      rowIndex: Int,
      expected: String,
      actual: String): String = {
    s"""
       |=== Row $rowIndex - $sqlNum result does NOT match expected ===
       |[Expected]
       |$expected
       |[Actual]
       |$actual
       |""".stripMargin
  }

  private def normalizeExplainPlan(plan: String): String = {
    val exprIdRegex = "#\\d+L?".r
    val planIdRegex = "plan_id=\\d+".r

    // Normalize file location
    def normalizeLocation(plan: String): String = {
      plan.replaceAll("""file:/[^,\s\]\)]+""", "file:/<warehouse_dir>")
    }

    // Create a normalized map for regex matches
    def createNormalizedMap(regex: Regex, plan: String): Map[String, String] = {
      val map = new mutable.HashMap[String, String]()
      regex
        .findAllMatchIn(plan)
        .map(_.toString)
        .foreach(map.getOrElseUpdate(_, (map.size + 1).toString))
      map.toMap
    }

    // Replace occurrences in the plan using the normalized map
    def replaceWithNormalizedValues(
        plan: String,
        regex: Regex,
        normalizedMap: Map[String, String],
        format: String): String = {
      regex.replaceAllIn(plan, regexMatch => s"$format${normalizedMap(regexMatch.toString)}")
    }

    // Normalize the entire plan step by step
    val exprIdMap = createNormalizedMap(exprIdRegex, plan)
    val exprIdNormalized = replaceWithNormalizedValues(plan, exprIdRegex, exprIdMap, "#")

    val planIdMap = createNormalizedMap(planIdRegex, exprIdNormalized)
    val planIdNormalized =
      replaceWithNormalizedValues(exprIdNormalized, planIdRegex, planIdMap, "plan_id=")

    // QueryStageExec will take its id as argument, replace it with X
    val argumentsNormalized = planIdNormalized
      .replaceAll("Arguments: [0-9]+, [0-9]+", "Arguments: X, X")
      .replaceAll("Arguments: [0-9]+", "Arguments: X")

    normalizeLocation(argumentsNormalized)
  }

  protected def verifyPlan(df: DataFrame, sqlNum: String, planPath: String): Unit = {
    if (!shouldVerifyPlan()) {
      return
    }
    val expectedFile = new File(planPath, s"$sqlNum.txt")
    val actual = normalizeExplainPlan(df.queryExecution.explainString(FormattedMode))

    if (RE_GENERATE) {
      FileUtils.writeStringToFile(expectedFile, actual, StandardCharsets.UTF_8)
      return
    }

    val expected = FileUtils.readFileToString(expectedFile, StandardCharsets.UTF_8)

    val actualFile = new File(FileUtils.getTempDirectory, s"tpch.plan.actual.$sqlNum.txt")
    FileUtils.writeStringToFile(actualFile, actual, StandardCharsets.UTF_8)

    if (expected != actual) {
      fail(s"""
              |Plans did not match for query: $sqlNum
              |Expected explain plan: ${expectedFile.getAbsolutePath}
              |
              |$expected
              |Actual explain plan: ${actualFile.getAbsolutePath}
              |
              |$actual
              |""".stripMargin)
    }
  }


    // FIXME
    // TODO q1 avg函数浮点数精度差异
    /*
    q1  与spark结果不一致, avg函数有差异
![A,F,380456.00,532348211.65,505822441.4861,526165934.000839,25.575155,35785.709307,0.050081,14876]   --- spark
 [A,F,380456.00,532348211.65,505822441.4861,526165934.000839,25.575154,35785.709306,0.050081,14876]   --- auron

    // TODO q17,q19  join支持join condition
    q17 存在非native算子
      25/11/30 17:25:54 WARN NativeConverters: Falling back expression: scala.NotImplementedError: unsupported expression: (class org.apache.spark.sql.catalyst.expressions.Multiply) (0.2 * avg(l_quantity#530)#524)
      25/11/30 17:25:54 WARN AuronConverters: Falling back exec: SortMergeJoinExec: assertion failed: join condition is not supported
      25/11/30 17:25:54 WARN NativeConverters: Falling back expression: scala.NotImplementedError: unsupported expression: (class org.apache.spark.sql.catalyst.expressions.Multiply) (0.2 * avg(l_quantity#530)#524)
      25/11/30 17:25:54 WARN NativeConverters: Falling back expression: scala.NotImplementedError: unsupported expression: (class org.apache.spark.sql.catalyst.expressions.Multiply) (0.2 * none#1)
      25/11/30 17:25:54 WARN NativeConverters: Falling back expression: scala.NotImplementedError: unsupported expression: (class org.apache.spark.sql.catalyst.expressions.Multiply) (0.2 * none#1)
      25/11/30 17:25:54 WARN NativeConverters: Falling back expression: scala.NotImplementedError: unsupported expression: (class org.apache.spark.sql.catalyst.expressions.Multiply) (0.2 * avg(l_quantity#530)#524)
                                               Falling back exec: HashAggregateExec: assertion failed: partial AggregateExec is not native

    q19 存在非native算子
    25/11/30 17:30:43 WARN AuronConverters: Falling back exec: SortMergeJoinExec: assertion failed: join condition is not supported
    25/11/30 17:30:43 WARN NativeConverters: Falling back expression: scala.NotImplementedError: unsupported expression: (class org.apache.spark.sql.catalyst.expressions.Subtract) (1 - l_discount#67)
    25/11/30 17:30:43 WARN NativeConverters: Falling back expression: scala.NotImplementedError: unsupported expression: (class org.apache.spark.sql.catalyst.expressions.Multiply) (class org.apache.spark.sql.auron
     */

  // only run single query
  if (false) {
    val sqlNum = "q1"
    test(s"query: $sqlNum") {
      val sqlStr = FileUtils.readFileToString(
        new File(s"$tpchQueriesPath/$sqlNum.sql"),
        StandardCharsets.UTF_8)
      verifyResult(sql(sqlStr), sqlNum, tpchResultsPath)
      verifyPlan(sql(sqlStr), sqlNum, tpchPlanPath)
      //checkSparkAnswerAndOperator(sqlStr)
      checkSparkAnswer(sqlStr)
      //Thread.sleep(100000000)
    }
  }

  // check query result and query plan
  if (true) {
    tpchQueries.foreach { sqlNum =>
      test("TPC-H " + sqlNum) {
        val sqlStr = FileUtils.readFileToString(
          new File(s"$tpchQueriesPath/$sqlNum.sql"),
          StandardCharsets.UTF_8)
        val df = spark.sql(sqlStr)
        verifyResult(df, sqlNum, tpchResultsPath)
        verifyPlan(df, sqlNum, tpchPlanPath)
        //checkSparkAnswerAndOperator(sqlStr)
        checkSparkAnswer(sqlStr)
      }
    }
  }
}

class AuronTPCHV1Suite extends AuronTPCHSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
}
