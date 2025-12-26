/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.auron.fuzz

import org.apache.auron.fuzz.QueryComparison.{showPlans, showResult}
import org.apache.auron.fuzz.old.NativeEngineConf
import org.apache.spark.sql.{Row, SparkSession}

import java.io.{BufferedWriter, FileWriter, PrintWriter, StringWriter}
import scala.collection.mutable
import scala.io.Source

object QueryRunner {

  def createOutputMdFile(): BufferedWriter = {
    //val outputFilename = s"results-${System.currentTimeMillis()}.md"
    val outputFilename = s"results.md"
    // scalastyle:off println
    println(s"Writing results to $outputFilename")
    // scalastyle:on println

    new BufferedWriter(new FileWriter(outputFilename))
  }

  def runQueries(
      spark: SparkSession,
      nativeEngineConf: NativeEngineConf,
      numFiles: Int,
      filename: String,
      showFailedSparkQueries: Boolean = false): Unit = {

    var queryCount = 0
    var invalidQueryCount = 0
    var naitveFailureCount = 0
    var naitveSuccessCount = 0

    val w = createOutputMdFile()

    // register input files
    for (i <- 0 until numFiles) {
      val table = spark.read.parquet(s"test$i.parquet")
      val tableName = s"test$i"
      table.createTempView(tableName)
      w.write(
        s"Created table $tableName with schema:\n\t" +
          s"${table.schema.fields.map(f => s"${f.name}: ${f.dataType}").mkString("\n\t")}\n\n")
    }

    val querySource = Source.fromFile(filename)
    try {
      querySource
        .getLines()
        .foreach(sql => {
          queryCount += 1
          try {
            println(s"Running query[${queryCount}]: $sql")

            // execute with Spark
            nativeEngineConf.disableNativeEngine(spark)
            val df = spark.sql(sql)
            val sparkRows = df.collect()
            val sparkPlan = df.queryExecution.executedPlan.toString

            // execute with native engion
            try {
              nativeEngineConf.enableNativeEngine(spark)
              val df = spark.sql(sql)
              val naitveRows = df.collect()
              val naitvePlan = df.queryExecution.executedPlan.toString

              var success = QueryComparison.assertSameRows(sparkRows, naitveRows, output = w)

              // check that the plan contains naitve operators
//              if (!naitvePlan.contains("naitve")) {
//                success = false
//                w.write("[ERROR] naitve did not accelerate any part of the plan\n")
//              }

              //QueryComparison.showSQL(w, sql)

              if (success) {
                naitveSuccessCount += 1
              } else {
                // show plans for failed queries
                QueryComparison.showSQL(w, sql, sqlNum = queryCount)
                showPlans(w, sparkPlan, naitvePlan)
                naitveFailureCount += 1
                showResult(w, sparkRows, naitveRows)
                w.write(s"```\n ------ sqlNum=$queryCount end ------ \\n\\n")
              }

            } catch {
              case e: Throwable =>
                // the query worked in Spark but failed in naitve, so this is likely a bug in naitve
                naitveFailureCount += 1
                QueryComparison.showSQL(w, sql,  sqlNum = queryCount)

                w.write("### Error Message\n")
                w.write("```\n")
                w.write(s"[ERROR] Query failed in native engine ${nativeEngineConf.engineName}: ${e.getMessage}:\n")
                w.write("```\n")

                w.write("### Error StackTrace\n")
                w.write("```\n")
                val sw = new StringWriter()
                val p = new PrintWriter(sw)
                e.printStackTrace(p)
                p.close()
                w.write(s"${sw.toString}\n")
                w.write(s"```\n ------ sqlNum=$queryCount end ------ \\n\\n")
            }

            // flush after every query so that results are saved in the event of the driver crashing
            w.flush()

          } catch {
            case e: Throwable =>
              // we expect many generated queries to be invalid
              invalidQueryCount += 1
              if (showFailedSparkQueries) {
                QueryComparison.showSQL(w, sql, sqlNum =  queryCount)
                w.write(s"Query failed in Spark: ${e.getMessage}\n ------ sqlNum=$queryCount end ------ \n\n")
              }
          }
        })

      w.write("# Summary\n")
      w.write(
        s"Total queries: $queryCount; Invalid queries: $invalidQueryCount; " +
          s"${nativeEngineConf.engineName} failed: $naitveFailureCount; ${nativeEngineConf.engineName} succeeded: $naitveSuccessCount\n")

    } finally {
      w.close()
      querySource.close()
    }
  }
}

object QueryComparison {
  def assertSameRows(
      sparkRows: Array[Row],
      naitveRows: Array[Row],
      output: BufferedWriter,
      tolerance: Double = 0.000001): Boolean = {
    if (sparkRows.length == naitveRows.length) {
      var i = 0
      while (i < sparkRows.length) {
        val l = sparkRows(i)
        val r = naitveRows(i)

        // Check the schema is equal for first row only
        if (i == 0 && l.schema != r.schema) {
          output.write("[ERROR] Spark produced different schema than Naitve.\n")

          return false
        }

        assert(l.length == r.length)
        for (j <- 0 until l.length) {
          if (!same(l(j), r(j), tolerance)) {
            output.write(s"First difference at row $i:\n")
            output.write("Spark: `" + formatRow(l) + "`\n")
            output.write("Naitve: `" + formatRow(r) + "`\n")
            i = sparkRows.length

            return false
          }
        }
        i += 1
      }
    } else {
      output.write(
        s"[ERROR] Spark produced ${sparkRows.length} rows and " +
          s"Naitve produced ${naitveRows.length} rows.\n")

      return false
    }

    true
  }

  private def same(l: Any, r: Any, tolerance: Double): Boolean = {
    if (l == null || r == null) {
      return l == null && r == null
    }
    (l, r) match {
      case (a: Float, b: Float) if a.isPosInfinity => b.isPosInfinity
      case (a: Float, b: Float) if a.isNegInfinity => b.isNegInfinity
      case (a: Float, b: Float) if a.isInfinity => b.isInfinity
      case (a: Float, b: Float) if a.isNaN => b.isNaN
      case (a: Float, b: Float) => (a - b).abs <= tolerance
      case (a: Double, b: Double) if a.isPosInfinity => b.isPosInfinity
      case (a: Double, b: Double) if a.isNegInfinity => b.isNegInfinity
      case (a: Double, b: Double) if a.isInfinity => b.isInfinity
      case (a: Double, b: Double) if a.isNaN => b.isNaN
      case (a: Double, b: Double) => (a - b).abs <= tolerance
      case (a: Array[_], b: Array[_]) =>
        a.length == b.length && a.zip(b).forall(x => same(x._1, x._2, tolerance))
      case (a: mutable.WrappedArray[_], b: mutable.WrappedArray[_]) =>
        a.length == b.length && a.zip(b).forall(x => same(x._1, x._2, tolerance))
      case (a: Row, b: Row) =>
        val aa = a.toSeq
        val bb = b.toSeq
        aa.length == bb.length && aa.zip(bb).forall(x => same(x._1, x._2, tolerance))
      case (a, b) => a == b
    }
  }

  private def format(value: Any): String = {
    value match {
      case null => "NULL"
      case v: mutable.WrappedArray[_] => s"[${v.map(format).mkString(",")}]"
      case v: Array[Byte] => s"[${v.mkString(",")}]"
      case v: Array[Row] => s"[${v.map(format).mkString(",")}]"
      case r: Row => formatRow(r)
      case other => other.toString
    }
  }

  private def formatRow(row: Row): String = {
    row.toSeq.map(format).mkString(",")
  }

  def showSQL(w: BufferedWriter, sql: String, maxLength: Int = 120, sqlNum: Int): Unit = {
    w.write(s"## SQL $sqlNum\n")
    w.write("```\n")
    val words = sql.split(" ")
    val currentLine = new StringBuilder
    for (word <- words) {
      if (currentLine.length + word.length + 1 > maxLength) {
        w.write(currentLine.toString.trim)
        w.write("\n")
        currentLine.setLength(0)
      }
      currentLine.append(word).append(" ")
    }
    if (currentLine.nonEmpty) {
      w.write(currentLine.toString.trim)
      w.write("\n")
    }
    w.write("```\n")
  }

  def showPlans(w: BufferedWriter, sparkPlan: String, naitvePlan: String): Unit = {
    w.write("### Spark Plan\n")
    w.write(s"```\n$sparkPlan\n```\n")
    w.write("### Naitve Plan\n")
    w.write(s"```\n$naitvePlan\n```\n")
  }

  def showSchema(w: BufferedWriter, sparkSchema: String, naitveSchema: String): Unit = {
    w.write("### Spark Schema\n")
    w.write(s"```\n$sparkSchema\n```\n")
    w.write("### Naitve Schema\n")
    w.write(s"```\n$naitveSchema\n```\n")
  }
  
  def showResult(w: BufferedWriter, sparkRows: Array[Row], naitveRows: Array[Row]): Unit = {
    w.write("### Spark Result\n")
    w.write(s"```\n${format(sparkRows)}\n```\n")
    w.write("### Naitve Result\n")
    w.write(s"```\n${format(naitveRows)}\n```\n")
  }
}
