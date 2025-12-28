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

import org.apache.auron.integration.{QueryRunner, Suite, SuiteArgs}
import org.apache.auron.integration.comparator.{ComparisonResult, PlanStability, QueryResultComparator}
import org.apache.auron.integration.tpcds.TPCDSFeatures
import org.apache.spark.sql.SparkSession

class AuronTPCDSSuite(args: SuiteArgs) extends Suite(args) with TPCDSFeatures {

  val queryRunner = new QueryRunner(loadQuerySql = (qid: String) => this.loadQuerySql(qid))

  val resutComparator = new QueryResultComparator()

  val planStability = new PlanStability(
    readGoldenPlan = (qid: String) => this.readGoldenPlan(qid),
    writeGoldenPlan = (qid: String, plan: String) => this.writeGoldenPlan(qid, plan),
    regenGoldenFiles = args.regenGoldenFiles,
    planCheck = args.enablePlanCheck)

  override def run(): Int = {
    val queries = if(args.queryFilter == Nil) {
      tpcdsQueries.toList
    } else {
      filterQueries(args.queryFilter)
    }

    if (queries.isEmpty) {
      println("No valid queries specified")
      return 1
    }
    println(s"AuronTPCDSSuite queries: $queries")

    setupTables(args.dataLocation, sessions.baselineSpark)

    val comparisonResults = executeBenchmark(queries)

    printComparisonResults(comparisonResults)

    val failed = comparisonResults.count(r => !r.success || !r.planStable)
    if (failed > 0) {
      println(s"\nTPC-DS test FAILED: $failed/${comparisonResults.length} queries failed")
      1
    } else {
      println(s"\nTPC-DS test PASSED: ${comparisonResults.length}/${comparisonResults.length}")
      0
    }
  }

  private def executeBenchmark(queries: List[String]): List[ComparisonResult] = {
    val baselineResults = queryRunner.runQueries(sessions.baselineSpark, queries)
    println("sessions.baselineSpark.stop ....")
    sessions.baselineSpark.stop()
    val testResults = queryRunner.runQueries(sessions.auronSpark, queries)

    val comparisonResults = queries.map { queryId =>
      resutComparator.compare(baselineResults(queryId), testResults(queryId))
    }

    if(args.enablePlanCheck || args.regenGoldenFiles) {
      comparisonResults.foreach(comparisonResult => {
        val testResult = testResults(comparisonResult.queryId)
        val planStable = planStability.validate(testResult)
        comparisonResult.planStable = planStable
      })
    }
    comparisonResults
  }

  private def printComparisonResults(results: List[ComparisonResult]): Unit = {
    println("\n" + "=" * 120)
    println("📊 TPC-DS Comparison Results")
    println("=" * 120)
    println("Query | Rows(B/T) | Time(B/T) | Speedup | Data | PlanStable")
    println("=" * 120)

    results.sortBy(_.speedup).reverse.foreach { r =>
      val status = if (r.success) "✅" else "❌"
      println(
        f"$status ${r.queryId}%-6s | ${r.baselineRows}%5d/${r.testRows}%5d | " +
          f"${r.baselineTime}%.2f/${r.testTime}%.2fs | ${r.speedup}%.2fx | " +
          s"${if (r.dataMatch) "✓" else "✗"} | " +
          s"${if (r.planStable) "✓" else "✗"}" )
    }

    val passed = results.count(_.success)
    val total = results.length
    println("=" * 120)
    println(f"Summary: $passed/$total PASSED (${passed * 100.0 / total})")
  }
}

object AuronTPCDSSuite {
  def apply(args: SuiteArgs): Suite = new AuronTPCDSSuite(args)
}
