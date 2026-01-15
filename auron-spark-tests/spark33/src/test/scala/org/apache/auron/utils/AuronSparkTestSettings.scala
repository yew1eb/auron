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
package org.apache.auron.utils

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{AuronAnsiCastSuite, AuronArithmeticExpressionSuite, AuronBitwiseExpressionsSuite, AuronCastSuite, AuronCollectionExpressionsSuite, AuronComplexTypeSuite, AuronConditionalExpressionSuite, AuronDateExpressionsSuite, AuronDecimalExpressionSuite, AuronDecimalPrecisionSuite, AuronGeneratorExpressionSuite, AuronHashExpressionsSuite, AuronHigherOrderFunctionsSuite, AuronIntervalExpressionsSuite, AuronJsonExpressionsSuite, AuronLiteralExpressionSuite, AuronMathExpressionsSuite, AuronMiscExpressionsSuite, AuronNondeterministicSuite, AuronNullExpressionsSuite, AuronPredicateSuite, AuronRandomSuite, AuronRegexpExpressionsSuite, AuronSortOrderExpressionsSuite, AuronStringExpressionsSuite}
import org.apache.spark.sql.catalyst.expressions.aggregate.AuronPercentileSuite
import org.apache.spark.sql.excution.{AuronBroadcastExchangeSuite, AuronExchangeSuite, AuronQueryExecutionSuite, AuronReuseExchangeAndSubquerySuite, AuronSQLAggregateFunctionSuite, AuronSQLCollectLimitExecSuite, AuronSQLWindowFunctionSuite, AuronSameResultSuite, AuronSortSuite, AuronTakeOrderedAndProjectSuite}
import org.apache.spark.sql.excution.adaptive.AuronTestAdaptiveQueryExecSuite
import org.apache.spark.sql.excution.joins.{AuronBroadcastJoinSuite, AuronExistenceJoinSuite, AuronInnerJoinSuite, AuronOuterJoinSuite}
import org.apache.spark.sql.excution.python.{GlutenBatchEvalPythonExecSuite, GlutenExtractPythonUDFsSuite}

class AuronSparkTestSettings extends SparkTestSettings {
  {
    // Use Arrow's unsafe implementation.
    System.setProperty("arrow.allocation.manager.type", "Unsafe")
  }

  enableSuite[AuronStringFunctionsSuite]
    // See https://github.com/apache/auron/issues/1724
    .exclude("string / binary substring function")

  enableSuite[AuronDataFrameAggregateSuite]
    // See https://github.com/apache/auron/issues/1840
    .excludeByPrefix("collect functions")
    // A custom version of the SPARK-19471 test has been added to AuronDataFrameAggregateSuite
    // with modified plan checks for Auron's native aggregates, so we exclude the original here.
    .exclude(
      "SPARK-19471: AggregationIterator does not initialize the generated result projection before using it")
    .exclude(
      "SPARK-24788: RelationalGroupedDataset.toString with unresolved exprs should not fail")

  enableSuite[AuronDatasetAggregatorSuite]

  enableSuite[AuronTypedImperativeAggregateSuite]

  enableSuite[AuronSubquerySuite]

  enableSuite[AuronSQLQuerySuite]

  enableSuite[AuronSQLQueryTestSuite]

  // Suites for JOINs.
  enableSuite[AuronExistenceJoinSuite]
    // See https://github.com/apache/auron/issues/1807
    .exclude(
      "test no condition with empty right side for left anti join using BroadcastNestedLoopJoin build left")

  enableSuite[AuronPercentileSuite]

  enableSuite[AuronBroadcastExchangeSuite]

  enableSuite[AuronAnsiCastSuite]

  enableSuite[AuronArithmeticExpressionSuite]

  enableSuite[AuronBitwiseExpressionsSuite]

  enableSuite[AuronCastSuite]

  enableSuite[AuronCollectionExpressionsSuite]

  enableSuite[AuronComplexTypeSuite]

  enableSuite[AuronConditionalExpressionSuite]

  enableSuite[AuronDateExpressionsSuite]


  enableSuite[AuronDecimalExpressionSuite]

  enableSuite[AuronDecimalPrecisionSuite]

  enableSuite[AuronGeneratorExpressionSuite]

  enableSuite[AuronHashExpressionsSuite]

  enableSuite[AuronHigherOrderFunctionsSuite]

  enableSuite[AuronIntervalExpressionsSuite]

  enableSuite[AuronJsonExpressionsSuite]

  enableSuite[AuronLiteralExpressionSuite]

  enableSuite[AuronMathExpressionsSuite]

  enableSuite[AuronMiscExpressionsSuite]

  enableSuite[AuronNondeterministicSuite]
  enableSuite[AuronNullExpressionsSuite]
  enableSuite[AuronPredicateSuite]
  enableSuite[AuronRandomSuite]
  enableSuite[AuronRegexpExpressionsSuite]
  enableSuite[AuronSortOrderExpressionsSuite]
  enableSuite[AuronStringExpressionsSuite]

  enableSuite[AuronTestAdaptiveQueryExecSuite]

  enableSuite[AuronBroadcastJoinSuite]

  enableSuite[AuronInnerJoinSuite]
  enableSuite[AuronOuterJoinSuite]

  enableSuite[GlutenBatchEvalPythonExecSuite]
  enableSuite[GlutenExtractPythonUDFsSuite]

  enableSuite[AuronExchangeSuite]
  enableSuite[AuronQueryExecutionSuite]
  enableSuite[AuronReuseExchangeAndSubquerySuite]
  enableSuite[AuronSameResultSuite]
  enableSuite[AuronSortSuite]
  enableSuite[AuronSQLAggregateFunctionSuite]
  enableSuite[AuronSQLCollectLimitExecSuite]
  enableSuite[AuronSQLWindowFunctionSuite]
  enableSuite[AuronTakeOrderedAndProjectSuite]

  // Will be implemented in the future.
  override def getSQLQueryTestSettings = new SQLQueryTestSettings {
    override def getResourceFilePath: String = ???

    override def getSupportedSQLQueryTests: Set[String] = ???

    override def getOverwriteSQLQueryTests: Set[String] = ???
  }
}
