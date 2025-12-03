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
package org.apache.auron.fuzz

class AuronFuzzAggregateSuite extends AurontFuzzTestBase {

  // FIXME
  // TODO
  // Found non-native operator: HashAggregate
  /*
  25/12/02 08:03:33 INFO FileSourceScanExec: Planning scan with bin packing, max size: 4484920 bytes, open cost is considered as scanning 4194304 bytes.
25/12/02 08:03:33 WARN AuronConverters: Falling back exec: FileSourceScanExec: Data type conversion not implemented TimestampNTZType
scala.NotImplementedError: Data type conversion not implemented TimestampNTZType
	at org.apache.spark.sql.auron.NativeConverters$.convertDataType(NativeConverters.scala:220)
	at org.apache.spark.sql.auron.NativeConverters$.convertField(NativeConverters.scala:230)
	at org.apache.spark.sql.auron.NativeConverters$.$anonfun$convertSchema$1(NativeConverters.scala:236)
	at scala.collection.Iterator.foreach(Iterator.scala:943)
	at scala.collection.Iterator.foreach$(Iterator.scala:943)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
   */
  test("count distinct - simple columns") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.schema.fields.filterNot(f => isComplexType(f.dataType)).map(_.name)) {
      val sql = s"SELECT count(distinct $col) FROM t1"
      //val (_, cometPlan) = checkSparkAnswer(sql)
      val cometPlan = checkSparkAnswer(sql).queryExecution.executedPlan
      //if (usingDataSourceExec) {
      if (false) {
        assert(1 == collectNativeScans(cometPlan).length)
      }

      checkSparkAnswerAndOperator(sql)
    }
  }

  // Aggregate by complex columns not yet supported
  // https://github.com/apache/datafusion-comet/issues/2382
  test("count distinct - complex columns") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.schema.fields.filter(f => isComplexType(f.dataType)).map(_.name)) {
      val sql = s"SELECT count(distinct $col) FROM t1"
      //val (_, cometPlan) = checkSparkAnswer(sql)
      val cometPlan = checkSparkAnswer(sql).queryExecution.executedPlan
      println(cometPlan)
      //if (usingDataSourceExec) {
      if (false) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }
  // FIXME
  // TODO
  // Falling back exec: HashAggregateExec: Data type conversion not implemented TimestampNTZType
  test("count distinct group by multiple column - simple columns ") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.schema.fields.filterNot(f => isComplexType(f.dataType)).map(_.name)) {
      val sql = s"SELECT c1, c2, c3, count(distinct $col) FROM t1 group by c1, c2, c3"
      //val (_, cometPlan) = checkSparkAnswer(sql)
      val cometPlan = checkSparkAnswer(sql).queryExecution.executedPlan
      //if (usingDataSourceExec) {
      if (false) {
        assert(1 == collectNativeScans(cometPlan).length)
      }

      checkSparkAnswerAndOperator(sql)
    }
  }

  // Aggregate by complex columns not yet supported
  // https://github.com/apache/datafusion-comet/issues/2382
  test("count distinct group by multiple column - complex columns ") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.schema.fields.filter(f => isComplexType(f.dataType)).map(_.name)) {
      val sql = s"SELECT c1, c2, c3, count(distinct $col) FROM t1 group by c1, c2, c3"
      //val (_, cometPlan) = checkSparkAnswer(sql)
      val cometPlan = checkSparkAnswer(sql).queryExecution.executedPlan
      println(cometPlan)
      //if (usingDataSourceExec) {
      if (false) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  // COUNT(distinct x, y, z, ...) not yet supported
  // https://github.com/apache/datafusion-comet/issues/2292
  test("count distinct multiple values and group by multiple column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      val sql = s"SELECT c1, c2, c3, count(distinct $col, c4, c5) FROM t1 group by c1, c2, c3"
      //val (_, cometPlan) = checkSparkAnswer(sql)
      val cometPlan = checkSparkAnswer(sql).queryExecution.executedPlan
      //if (usingDataSourceExec) {
      if (false) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("count(*) group by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      val sql = s"SELECT $col, count(*) FROM t1 GROUP BY $col ORDER BY $col"
      //val (_, cometPlan) = checkSparkAnswer(sql)
      val cometPlan = checkSparkAnswer(sql).queryExecution.executedPlan
      //if (usingDataSourceExec) {
      if (false) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("count(col) group by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val groupCol = df.columns.head
    for (col <- df.columns.drop(1)) {
      val sql = s"SELECT $groupCol, count($col) FROM t1 GROUP BY $groupCol ORDER BY $groupCol"
      //val (_, cometPlan) = checkSparkAnswer(sql)
      val cometPlan = checkSparkAnswer(sql).queryExecution.executedPlan
      //if (usingDataSourceExec) {
      if (false) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }

  test("count(col1, col2, ..) group by single column") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val groupCol = df.columns.head
    val otherCol = df.columns.drop(1)
    val sql = s"SELECT $groupCol, count(${otherCol.mkString(", ")}) FROM t1 " +
      s"GROUP BY $groupCol ORDER BY $groupCol"
    //val (_, cometPlan) = checkSparkAnswer(sql)
    val cometPlan = checkSparkAnswer(sql).queryExecution.executedPlan
    //if (usingDataSourceExec) {
    if (false) {
      assert(1 == collectNativeScans(cometPlan).length)
    }
  }

  // FIXME
  // TODO
  // == Results ==
  //!== Correct Answer - 1 ==              == Spark Answer - 1 ==
  // struct<min(c5):float,max(c5):float>   struct<min(c5):float,max(c5):float>
  //![-Infinity,NaN]                       [-Infinity,Infinity]
  test("min/max aggregate") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    for (col <- df.columns) {
      // cannot run fully native due to HashAggregate
      val sql = s"SELECT min($col), max($col) FROM t1"
      //val (_, cometPlan) = checkSparkAnswer(sql)
      val cometPlan = checkSparkAnswer(sql).queryExecution.executedPlan
      //if (usingDataSourceExec) {
      if (false) {
        assert(1 == collectNativeScans(cometPlan).length)
      }
    }
  }
}
