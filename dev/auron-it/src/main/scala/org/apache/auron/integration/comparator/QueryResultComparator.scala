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
package org.apache.auron.integration.comparator

import org.apache.spark.sql.Row

import org.apache.auron.integration.SingleQueryResult

class QueryResultComparator extends QueryComparator {
  private val colSep = "<|COL|>"

  override def compare(baseline: SingleQueryResult, test: SingleQueryResult): ComparisonResult = {
    val rowMatch = baseline.rowCount == test.rowCount
    val dataMatch = compareDataPrecisely(baseline.rows, test.rows)

    val speedup = if (baseline.durationSec > 0) test.durationSec / baseline.durationSec else 0.0
    val success = rowMatch && dataMatch && (baseline.success == test.success)

    ComparisonResult(
      baseline.queryId,
      baseline.rowCount,
      test.rowCount,
      baseline.durationSec,
      test.durationSec,
      rowMatch,
      dataMatch,
      speedup,
      success)
  }

  private def compareDataPrecisely(baselineRows: Array[Row], testRows: Array[Row]): Boolean = {
    if (baselineRows.length != testRows.length) return false

    val baselineContent = formatResultContent(baselineRows)
    val testContent = formatResultContent(testRows)
    baselineContent == testContent
  }

  private def formatResultContent(rows: Array[Row]): String = {
    val rowStrings = rows.map(_.mkString(colSep))
    s"${rows.length}\n${rowStrings.mkString("\n")}\n"
  }
}
