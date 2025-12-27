package org.apache.auron.integration.tpcds

import org.apache.auron.integration.runner.AuronTPCDSSuite.{colSep, regenGoldenFiles, tpcdsPlanPath, tpcdsResultsPath}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.types.DoubleType

import java.io.File
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.util.matching.Regex

object TPCDSQueriesCompare {


  def shouldVerifyPhysicalPlan(): Boolean = {
    Shims.get.shimVersion match {
      case "spark-3.5" => true
      case _ => false // TODO: Support for other Spark versions in the future
    }
  }

  protected def checkQueryResult(df: DataFrame, queryId: String): Unit = {
    val goldenFile = new File(s"$tpcdsResultsPath/$queryId.out")
    val rows = df.collect()

    if (regenGoldenFiles) {
      writeGoldenFile(goldenFile, formatResultContent(rows))
      return
    }

    if (df.schema.exists(_.dataType == DoubleType)) {
      compareDoubleResult(queryId, rows, goldenFile)
    } else {
      compareResultStr(queryId, rows, goldenFile)
    }
  }

  private def formatResultContent(rows: Array[Row]): String = {
    val rowStrings = rows.map(_.mkString(colSep))
    s"${rows.length}\n${rowStrings.mkString("\n")}\n"
  }

  private def writeGoldenFile(file: File, content: String): Unit = {
    Option(file.getParentFile).foreach(_.mkdirs())
    FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8)
  }

  protected def compareResultStr(sqlNum: String, rows: Array[Row], goldenFile: File): Unit = {
    val actualContent = formatResultContent(rows)

    val expectedResult = FileUtils.readFileToString(goldenFile, StandardCharsets.UTF_8)
    if (expectedResult != actualContent) {
      fail(s"""
              |=== $sqlNum result does NOT match expected ===
              |[Expected]
              |${expectedResult}
              |[Actual]
              |${actualContent}
              |""".stripMargin)
    }
  }

  protected def compareDoubleResult(
                                     queryId: String,
                                     rows: Array[Row],
                                     goldenFile: File,
                                     tolerance: Double = 1e-6): Unit = {

    val expectedRowIter = FileUtils.readLines(goldenFile, StandardCharsets.UTF_8).iterator()
    val expectedRowCount = expectedRowIter.next().toInt
    assert(
      rows.length == expectedRowCount,
      s"Row count mismatch in $queryId: expected $expectedRowCount, got ${rows.length}")

    rows.zipWithIndex.foreach { case (actualRow, rowIdx) =>
      assert(expectedRowIter.hasNext)
      val expectedRow = expectedRowIter.next().split(Regex.quote(colSep))

      actualRow.schema.zipWithIndex.foreach { case (field, colIdx) =>
        if (actualRow.isNullAt(colIdx)) {
          assert("null".equals(expectedRow(colIdx)))
        } else {
          field.dataType match {
            case DoubleType =>
              val actualValue = actualRow.getDouble(colIdx)
              val expectedValue = expectedRow(colIdx).toDouble
              val diff = math.abs(actualValue - expectedValue)
              assert(
                diff < tolerance,
                s"Floating-point mismatch in $queryId row $rowIdx col $colIdx: " +
                  s"expected ${expectedValue}, got ${actualValue} (diff=$diff)")
            case _ =>
              val actualValue = actualRow.get(colIdx).toString
              val expectedValue = expectedRow(colIdx)
              assert(
                actualValue == expectedValue,
                s"Mismatch in $queryId row $rowIdx col $colIdx: " +
                  s"expected $expectedValue, got $actualValue")
          }
        }
      }
    }
  }

  private def normalizePhysicalPlan(plan: String): String = {
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

  protected def checkPhysicalPlan(df: DataFrame, queryId: String): Unit = {
    if (!shouldVerifyPhysicalPlan()) {
      return
    }

    val goldenPlanFile = new File(s"$tpcdsPlanPath/$queryId.txt")
    val actualPlan = normalizePhysicalPlan(df.queryExecution.explainString(FormattedMode))

    if (regenGoldenFiles) {
      writeGoldenFile(goldenPlanFile, actualPlan)
      return
    }

    val expectedPlan = FileUtils.readFileToString(goldenPlanFile, StandardCharsets.UTF_8)
    if (expectedPlan != actualPlan) {
      val actualTempFile = new File(FileUtils.getTempDirectory, s"tpch.actual.plan.$queryId.txt")
      FileUtils.writeStringToFile(actualTempFile, actualPlan, StandardCharsets.UTF_8)
      fail(s"""
              |Physical plan mismatch for query $queryId
              |Expected: ${goldenPlanFile.getAbsolutePath}
              |Actual  : ${actualTempFile.getAbsolutePath}
              |
              |--- Expected ---
              |$expectedPlan
              |
              |--- Actual ---
              |$actualPlan
              |""".stripMargin)
    }
  }
   def runQueries(session: SparkSession, queryLocation: String, queries: Seq[String]): Unit = {
    queries.foreach { queryId =>
      val queryFile = new File(s"$queryLocation/$queryId.sql")
      val sqlText = FileUtils.readFileToString(queryFile, StandardCharsets.UTF_8).trim

      val resultDf = session.sql(sqlText)

      checkQueryResult(resultDf, queryId)
      checkPhysicalPlan(resultDf, queryId)
    }
  }
}
