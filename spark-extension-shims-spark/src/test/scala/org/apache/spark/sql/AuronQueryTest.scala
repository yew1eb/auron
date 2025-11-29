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
package org.apache.spark.sql

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{SimpleGroup, SimpleGroupFactory}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.schema.{MessageType, MessageTypeParser}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Success, Try}
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, MapType, StructType}
import org.scalatest.BeforeAndAfterEach

import java.util.concurrent.atomic.AtomicInteger

/**
 * Base test class under org.apache.spark.sql to use package-private [[SQLTestUtils]]; extends
 * [[QueryTest]] for comparisons and checks.
 */
abstract class AuronQueryTest
    extends QueryTest
    with SQLTestUtils
    with BeforeAndAfterEach
    with AdaptiveSparkPlanHelper {

  /**
   * Assert results match vanilla Spark, skip operator checks.
   */
  protected def checkSparkAnswer(sqlStr: String): DataFrame = {
    checkSparkAnswerAndOperator(() => sql(sqlStr), requireNative = false)
  }

  protected def checkSparkAnswer(df: DataFrame): DataFrame = {
    checkSparkAnswerAndOperator(() => datasetOfRows(spark, df.logicalPlan), requireNative = false)
  }

  /**
   * Assert results match vanilla Spark, fail if any operator is not native.
   */
  protected def checkSparkAnswerAndOperator(sqlStr: String): DataFrame = {
    checkSparkAnswerAndOperator(() => sql(sqlStr), requireNative = true)
  }

  protected def checkSparkAnswerAndOperator(df: DataFrame): DataFrame = {
    checkSparkAnswerAndOperator(() => datasetOfRows(spark, df.logicalPlan))
  }

  /**
   * Assert results match vanilla Spark, fail if any operator is not native.
   */
  protected def checkSparkAnswerAndOperator(
      dataframe: () => DataFrame,
      requireNative: Boolean = true): DataFrame = {

    var expected: Seq[Row] = null
    withSQLConf("spark.auron.enable" -> "false") {
      val dfSpark = dataframe()
      expected = dfSpark.collect()
    }

    val dfAuron = dataframe()
    checkAnswer(dfAuron, expected)

    if (requireNative) {
      val plan = stripAQEPlan(dfAuron.queryExecution.executedPlan)
      plan
        .collectFirst { case op if !isNativeOrPassThrough(op) => op }
        .foreach { op: SparkPlan =>
          fail(s"""
               |Found non-native operator: ${op.nodeName}
               |plan: ${plan}""".stripMargin)
        }
    }

    dfAuron
  }

  protected def isNativeOrPassThrough(op: SparkPlan): Boolean = op match {
    case _: NativeSupports => true
    case e: UnaryExecNode
        if Seq("QueryStage", "InputAdapter", "CustomShuffleRead", "AQEShuffleRead")
          .exists(e.nodeName.contains) || e.nodeName.startsWith("WholeStageCodegen") =>
      true
    case e: LeafExecNode
        if Seq("ShuffleQueryStage", "BroadcastQueryStage").exists(e.nodeName.contains) =>
      true
    case _ => false
  }

  def datasetOfRows(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, plan)
  }

  /** Check for the correct results as well as the expected fallback reason */
  protected def checkSparkAnswerAndFallbackReason(
                                                   query: String,
                                                   fallbackReason: String): (SparkPlan, SparkPlan) = {
    checkSparkAnswerAndFallbackReasons(sql(query), Set(fallbackReason))
  }

  /** Check for the correct results as well as the expected fallback reason */
  protected def checkSparkAnswerAndFallbackReason(
                                                   df: => DataFrame,
                                                   fallbackReason: String): (SparkPlan, SparkPlan) = {
    checkSparkAnswerAndFallbackReasons(df, Set(fallbackReason))
  }

  /** Check for the correct results as well as the expected fallback reasons */
  protected def checkSparkAnswerAndFallbackReasons(
                                                    query: String,
                                                    fallbackReasons: Set[String]): (SparkPlan, SparkPlan) = {
    checkSparkAnswerAndFallbackReasons(sql(query), fallbackReasons)
  }

  /** Check for the correct results as well as the expected fallback reasons */
  protected def checkSparkAnswerAndFallbackReasons(
                                                    df: => DataFrame,
                                                    fallbackReasons: Set[String]): (SparkPlan, SparkPlan) = {
    checkSparkAnswer(df)
    (df.queryExecution.executedPlan, df.queryExecution.executedPlan)
  }


  protected def checkSparkAnswerMaybeThrows(
      df: => DataFrame): (Option[Throwable], Option[Throwable]) = {
    var expected: Try[Array[Row]] = null
    withSQLConf("spark.auron.enable" -> "false") {
      expected = Try(datasetOfRows(spark, df.logicalPlan).collect())
    }
    val actual = Try(datasetOfRows(spark, df.logicalPlan).collect())

    (expected, actual) match {
      case (Success(_), Success(_)) =>
        // compare results and confirm that they match
        checkSparkAnswerAndOperator(() => df)
        (None, None)
      case _ =>
        (expected.failed.toOption, actual.failed.toOption)
    }
  }


  protected def readResourceParquetFile(name: String): DataFrame = {
    spark.read.parquet(getResourceParquetFilePath(name))
  }

  protected def getResourceParquetFilePath(name: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(name).toString
  }

  protected def withParquetDataFrame[T <: Product: ClassTag: TypeTag](
                                                                       data: Seq[T],
                                                                       withDictionary: Boolean = true,
                                                                       schema: Option[StructType] = None)(f: DataFrame => Unit): Unit = {
    withParquetFile(data, withDictionary)(path => readParquetFile(path, schema)(f))
  }

  protected def withParquetTable[T <: Product: ClassTag: TypeTag](
                                                                   data: Seq[T],
                                                                   tableName: String,
                                                                   withDictionary: Boolean = true)(f: => Unit): Unit = {
    withParquetDataFrame(data, withDictionary) { df =>
      df.createOrReplaceTempView(tableName)
      withTempView(tableName)(f)
    }
  }

  protected def withParquetTable(df: DataFrame, tableName: String)(f: => Unit): Unit = {
    df.createOrReplaceTempView(tableName)
    withTempView(tableName)(f)
  }

  protected def withParquetTable(path: String, tableName: String)(f: => Unit): Unit = {
    val df = spark.read.format("parquet").load(path)
    withParquetTable(df, tableName)(f)
  }

  protected def withParquetFile[T <: Product: ClassTag: TypeTag](
                                                                  data: Seq[T],
                                                                  withDictionary: Boolean = true)(f: String => Unit): Unit = {
    withTempPath { file =>
      spark
        .createDataFrame(data)
        .write
        .option("parquet.enable.dictionary", withDictionary.toString)
        .parquet(file.getCanonicalPath)
      f(file.getCanonicalPath)
    }
  }

  protected def readParquetFile(path: String, schema: Option[StructType] = None)(
    f: DataFrame => Unit): Unit = schema match {
    case Some(s) => f(spark.read.format("parquet").schema(s).load(path))
    case None => f(spark.read.format("parquet").load(path))
  }

  protected def createParquetWriter(
                                     schema: MessageType,
                                     path: Path,
                                     dictionaryEnabled: Boolean = false,
                                     pageSize: Int = 1024,
                                     dictionaryPageSize: Int = 1024,
                                     pageRowCountLimit: Int = ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT,
                                     rowGroupSize: Long = 1024 * 1024L): ParquetWriter[Group] = {
    val hadoopConf = spark.sessionState.newHadoopConf()

    ExampleParquetWriter
      .builder(path)
      .withDictionaryEncoding(dictionaryEnabled)
      .withType(schema)
      // TODO we need to shim this and use withRowGroupSize(Long) with later parquet-hadoop versions to remove
      // the deprecated warning here
      .withRowGroupSize(rowGroupSize.toInt)
      .withPageSize(pageSize)
      .withDictionaryPageSize(dictionaryPageSize)
      .withPageRowCountLimit(pageRowCountLimit)
      .withConf(hadoopConf)
      .build()
  }

  // Maps `i` to both positive and negative to test timestamp after and before the Unix epoch
  protected def getValue(i: Long, div: Long): Long = {
    val value = if (i % 2 == 0) i else -i
    value % div
  }

  def makeParquetFileAllPrimitiveTypes(path: Path, dictionaryEnabled: Boolean, n: Int): Unit = {
    makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 0, n)
  }

  def getPrimitiveTypesParquetSchema: String = {
      s"""
        |message root {
        |  optional boolean                  _1;
        |  optional int32                    _2(INT_8);
        |  optional int32                    _3(INT_16);
        |  optional int32                    _4;
        |  optional int64                    _5;
        |  optional float                    _6;
        |  optional double                   _7;
        |  optional binary                   _8(UTF8);
        |  optional int32                    _9(UINT_8);
        |  optional int32                    _10(UINT_16);
        |  optional int32                    _11(UINT_32);
        |  optional int64                    _12(UINT_64);
        |  optional binary                   _13(ENUM);
        |  optional FIXED_LEN_BYTE_ARRAY(3)  _14;
        |  optional int32                    _15(DECIMAL(5, 2));
        |  optional int64                    _16(DECIMAL(18, 10));
        |  optional FIXED_LEN_BYTE_ARRAY(16) _17(DECIMAL(38, 37));
        |  optional INT64                    _18(TIMESTAMP(MILLIS,true));
        |  optional INT64                    _19(TIMESTAMP(MICROS,true));
        |  optional INT32                    _20(DATE);
        |  optional binary                   _21;
        |  optional INT32                    _id;
        |}
      """.stripMargin
  }

  def makeParquetFileAllPrimitiveTypes(
                                        path: Path,
                                        dictionaryEnabled: Boolean,
                                        begin: Int,
                                        end: Int,
                                        nullEnabled: Boolean = true,
                                        pageSize: Int = 128,
                                        randomSize: Int = 0): Unit = {
    // alwaysIncludeUnsignedIntTypes means we include unsignedIntTypes in the test even if the
    // reader does not support them
    val schemaStr = getPrimitiveTypesParquetSchema

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(
      schema,
      path,
      dictionaryEnabled = dictionaryEnabled,
      pageSize = pageSize,
      dictionaryPageSize = pageSize)

    val idGenerator = new AtomicInteger(0)

    val rand = scala.util.Random
    val data = (begin until end).map { i =>
      if (nullEnabled && rand.nextBoolean()) {
        None
      } else {
        if (dictionaryEnabled) Some(i % 4) else Some(i)
      }
    }
    data.foreach { opt =>
      val record = new SimpleGroup(schema)
      opt match {
        case Some(i) =>
          record.add(0, i % 2 == 0)
          record.add(1, i.toByte)
          record.add(2, i.toShort)
          record.add(3, i)
          record.add(4, i.toLong)
          record.add(5, i.toFloat)
          record.add(6, i.toDouble)
          record.add(7, i.toString * 48)
          record.add(8, (-i).toByte)
          record.add(9, (-i).toShort)
          record.add(10, -i)
          record.add(11, (-i).toLong)
          record.add(12, i.toString)
          record.add(13, ((i % 10).toString * 3).take(3))
          record.add(14, i)
          record.add(15, i.toLong)
          record.add(16, ((i % 10).toString * 16).take(16))
          record.add(17, i.toLong)
          record.add(18, i.toLong)
          record.add(19, i)
          record.add(20, i.toString)
          record.add(21, idGenerator.getAndIncrement())
        case _ =>
      }
      writer.write(record)
    }
    (0 until randomSize).foreach { _ =>
      val i = rand.nextLong()
      val record = new SimpleGroup(schema)
      record.add(0, i % 2 == 0)
      record.add(1, i.toByte)
      record.add(2, i.toShort)
      record.add(3, i.toInt)
      record.add(4, i)
      record.add(5, java.lang.Float.intBitsToFloat(i.toInt))
      record.add(6, java.lang.Double.longBitsToDouble(i))
      record.add(7, i.toString * 24)
      record.add(8, (-i).toByte)
      record.add(9, (-i).toShort)
      record.add(10, (-i).toInt)
      record.add(11, -i)
      record.add(12, i.toString)
      record.add(13, i.toString.take(3).padTo(3, '0'))
      record.add(14, i.toInt % 100000)
      record.add(15, i % 1000000000000000000L)
      record.add(16, i.toString.take(16).padTo(16, '0'))
      record.add(17, i)
      record.add(18, i)
      record.add(19, i.toInt)
      record.add(20, i.toString)
      record.add(21, idGenerator.getAndIncrement())
      writer.write(record)
    }

    writer.close()
  }

  protected def makeRawTimeParquetFileColumns(
                                               path: Path,
                                               dictionaryEnabled: Boolean,
                                               n: Int,
                                               rowGroupSize: Long = 1024 * 1024L): Seq[Option[Long]] = {
    val schemaStr =
      """
        |message root {
        |  optional int64 _0(INT_64);
        |  optional int64 _1(INT_64);
        |  optional int64 _2(INT_64);
        |  optional int64 _3(INT_64);
        |  optional int64 _4(INT_64);
        |  optional int64 _5(INT_64);
        |}
        """.stripMargin

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(
      schema,
      path,
      dictionaryEnabled = dictionaryEnabled,
      rowGroupSize = rowGroupSize)
    val div = if (dictionaryEnabled) 10 else n // maps value to a small range for dict to kick in

    val rand = scala.util.Random
    val expected = (0 until n).map { i =>
      if (rand.nextBoolean()) {
        None
      } else {
        Some(getValue(i, div))
      }
    }
    expected.foreach { opt =>
      val record = new SimpleGroup(schema)
      opt match {
        case Some(i) =>
          record.add(0, i)
          record.add(1, i * 1000) // convert millis to micros, same below
          record.add(2, i)
          record.add(3, i)
          record.add(4, i * 1000)
          record.add(5, i * 1000)
        case _ =>
      }
      writer.write(record)
    }

    writer.close()
    expected
  }

  // Creates Parquet file of timestamp values
  protected def makeRawTimeParquetFile(
                                        path: Path,
                                        dictionaryEnabled: Boolean,
                                        n: Int,
                                        rowGroupSize: Long = 1024 * 1024L): Seq[Option[Long]] = {
    val schemaStr =
      """
        |message root {
        |  optional int64 _0(TIMESTAMP_MILLIS);
        |  optional int64 _1(TIMESTAMP_MICROS);
        |  optional int64 _2(TIMESTAMP(MILLIS,true));
        |  optional int64 _3(TIMESTAMP(MILLIS,false));
        |  optional int64 _4(TIMESTAMP(MICROS,true));
        |  optional int64 _5(TIMESTAMP(MICROS,false));
        |  optional int64 _6(INT_64);
        |}
        """.stripMargin

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(
      schema,
      path,
      dictionaryEnabled = dictionaryEnabled,
      rowGroupSize = rowGroupSize)
    val div = if (dictionaryEnabled) 10 else n // maps value to a small range for dict to kick in

    val rand = scala.util.Random
    val expected = (0 until n).map { i =>
      if (rand.nextBoolean()) {
        None
      } else {
        Some(getValue(i, div))
      }
    }
    expected.foreach { opt =>
      val record = new SimpleGroup(schema)
      opt match {
        case Some(i) =>
          record.add(0, i)
          record.add(1, i * 1000) // convert millis to micros, same below
          record.add(2, i)
          record.add(3, i)
          record.add(4, i * 1000)
          record.add(5, i * 1000)
          record.add(6, i * 1000)
        case _ =>
      }
      writer.write(record)
    }

    writer.close()
    expected
  }

  // Generate a file based on a complex schema. Schema derived from https://arrow.apache.org/blog/2022/10/17/arrow-parquet-encoding-part-3/
  def makeParquetFileComplexTypes(
                                   path: Path,
                                   dictionaryEnabled: Boolean,
                                   numRows: Integer = 10000): Unit = {
    val schemaString =
      """
      message ComplexDataSchema {
        optional group optional_array (LIST) {
          repeated group list {
            optional int32 element;
          }
        }
        required group array_of_struct (LIST) {
          repeated group list {
            optional group struct_element {
              required int32 field1;
              optional group optional_nested_array (LIST) {
                repeated group list {
                  required int32 element;
                }
              }
            }
          }
        }
        optional group optional_map (MAP) {
          repeated group key_value {
            required int32 key;
            optional int32 value;
          }
        }
        required group complex_map (MAP) {
          repeated group key_value {
            required group map_key {
              required int32 key_field1;
              optional int32 key_field2;
            }
            required group map_value {
              required int32 value_field1;
              repeated int32 value_field2;
            }
          }
        }
      }
    """

    val schema: MessageType = MessageTypeParser.parseMessageType(schemaString)
    GroupWriteSupport.setSchema(schema, spark.sparkContext.hadoopConfiguration)

    val writer = createParquetWriter(schema, path, dictionaryEnabled)

    val groupFactory = new SimpleGroupFactory(schema)

    for (i <- 0 until numRows) {
      val record = groupFactory.newGroup()

      // Optional array of optional integers
      if (i % 2 == 0) { // optional_array for every other row
        val optionalArray = record.addGroup("optional_array")
        for (j <- 0 until (i % 5)) {
          val elementGroup = optionalArray.addGroup("list")
          if (j % 2 == 0) { // some elements are optional
            elementGroup.append("element", j)
          }
        }
      }

      // Required array of structs
      val arrayOfStruct = record.addGroup("array_of_struct")
      for (j <- 0 until (i % 3) + 1) { // Add one to three elements
        val structElementGroup = arrayOfStruct.addGroup("list").addGroup("struct_element")
        structElementGroup.append("field1", i * 10 + j)

        // Optional nested array
        if (j % 2 != 0) { // optional nested array for every other struct
          val nestedArray = structElementGroup.addGroup("optional_nested_array")
          for (k <- 0 until (i % 4)) { // Add zero to three elements.
            val nestedElementGroup = nestedArray.addGroup("list")
            nestedElementGroup.append("element", i + j + k)
          }
        }
      }

      // Optional map
      if (i % 3 == 0) { // optional_map every third row
        val optionalMap = record.addGroup("optional_map")
        optionalMap
          .addGroup("key_value")
          .append("key", i)
          .append("value", i)
        if (i % 5 == 0) { // another optional entry
          optionalMap
            .addGroup("key_value")
            .append("key", i)
          // Value is optional
          if (i % 10 == 0) {
            optionalMap
              .addGroup("key_value")
              .append("key", i)
              .append("value", i)
          }
        }
      }

      // Required map with complex key and value types
      val complexMap = record.addGroup("complex_map")
      val complexMapKeyVal = complexMap.addGroup("key_value")

      complexMapKeyVal
        .addGroup("map_key")
        .append("key_field1", i)

      complexMapKeyVal
        .addGroup("map_value")
        .append("value_field1", i)
        .append("value_field2", i * 100)
        .append("value_field2", i * 101)
        .append("value_field2", i * 102)

      writer.write(record)
    }

    writer.close()
  }

  protected def makeDateTimeWithFormatTable(
                                             path: Path,
                                             dictionaryEnabled: Boolean,
                                             n: Int,
                                             rowGroupSize: Long = 1024 * 1024L): Seq[Option[Long]] = {
    val schemaStr =
      """
        |message root {
        |  optional int64 _0(TIMESTAMP_MILLIS);
        |  optional int64 _1(TIMESTAMP_MICROS);
        |  optional int64 _2(TIMESTAMP(MILLIS,true));
        |  optional int64 _3(TIMESTAMP(MILLIS,false));
        |  optional int64 _4(TIMESTAMP(MICROS,true));
        |  optional int64 _5(TIMESTAMP(MICROS,false));
        |  optional int64 _6(INT_64);
        |  optional int32 _7(DATE);
        |  optional binary format(UTF8);
        |  optional binary dateFormat(UTF8);
        |  }
      """.stripMargin

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(
      schema,
      path,
      dictionaryEnabled = dictionaryEnabled,
      rowGroupSize = rowGroupSize)
    val div = if (dictionaryEnabled) 10 else n // maps value to a small range for dict to kick in

    val expected = (0 until n).map { i =>
      Some(getValue(i, div))
    }
    expected.foreach { opt =>
      val timestampFormats = List(
        "YEAR",
        "YYYY",
        "YY",
        "MON",
        "MONTH",
        "MM",
        "QUARTER",
        "WEEK",
        "DAY",
        "DD",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MILLISECOND",
        "MICROSECOND")
      val dateFormats = List("YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "QUARTER", "WEEK")
      val formats = timestampFormats.zipAll(dateFormats, "NONE", "YEAR")

      formats.foreach { format =>
        val record = new SimpleGroup(schema)
        opt match {
          case Some(i) =>
            record.add(0, i)
            record.add(1, i * 1000) // convert millis to micros, same below
            record.add(2, i)
            record.add(3, i)
            record.add(4, i * 1000)
            record.add(5, i * 1000)
            record.add(6, i * 1000)
            record.add(7, i.toInt)
            record.add(8, format._1)
            record.add(9, format._2)
          case _ =>
        }
        writer.write(record)
      }
    }
    writer.close()
    expected
  }

  def stripRandomPlanParts(plan: String): String = {
    plan.replaceFirst("file:.*,", "").replaceAll(raw"#\d+", "")
  }

  /**
   * The test encapsulates integration Comet test and does following:
   *   - prepares data using SELECT query and saves it to the Parquet file in temp folder
   *   - creates a temporary table with name `tableName` on top of temporary parquet file
   *   - runs the query `testQuery` reading data from `tableName`
   *
   * Asserts the `testQuery` data with Comet is the same is with Apache Spark and also asserts
   * only Comet operator are in the physical plan
   *
   * Example:
   *
   * {{{
   *  test("native reader - read simple ARRAY fields with SHORT field") {
   *     testSingleLineQuery(
   *       """
   *         |select array(cast(1 as short)) arr
   *         |""".stripMargin,
   *       "select arr from tbl",
   *       sqlConf = Seq(
   *         CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key -> "false",
   *         "spark.comet.explainFallback.enabled" -> "false"
   *       ),
   *       debugCometDF = df => {
   *         df.printSchema()
   *         df.explain("extended")
   *         df.show()
   *       })
   *   }
   * }}}
   *
   * @param prepareQuery
   *   prepare sample data with Comet disabled
   * @param testQuery
   *   the query to test. Typically with Comet enabled + other SQL config applied
   * @param testName
   *   test name
   * @param tableName
   *   table name where sample data stored
   * @param sqlConf
   *   additional spark sql configuration
   * @param debugCometDF
   *   optional debug access to DataFrame for `testQuery`
   */
  def testSingleLineQuery(
                           prepareQuery: String,
                           testQuery: String,
                           testName: String = "test",
                           tableName: String = "tbl",
                           sqlConf: Seq[(String, String)] = Seq.empty,
                           readSchema: Option[StructType] = None,
                           debugCometDF: DataFrame => Unit = _ => (),
                           checkCometOperator: Boolean = true): Unit = {

    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, testName).toUri.toString
      var data: java.util.List[Row] = new java.util.ArrayList()
      var schema: StructType = null

      withSQLConf("spark.auron.enable" -> "false") {
        val df = spark.sql(prepareQuery)
        data = df.collectAsList()
        schema = df.schema
      }

      spark.createDataFrame(data, schema).repartition(1).write.parquet(path)
      readParquetFile(path, readSchema.orElse(Some(schema))) { df =>
        df.createOrReplaceTempView(tableName)
      }

      withSQLConf(sqlConf: _*) {
        val cometDF = sql(testQuery)
        debugCometDF(cometDF)
        if (checkCometOperator) checkSparkAnswerAndOperator(() => cometDF)
        else checkSparkAnswerAndOperator(() => cometDF, requireNative = false)
      }
    }
  }

  def showString[T](
                     df: Dataset[T],
                     _numRows: Int,
                     truncate: Int = 20,
                     vertical: Boolean = false): String = {
    df.showString(_numRows, truncate, vertical)
  }

  def makeParquetFile(
                       path: Path,
                       total: Int,
                       numGroups: Int,
                       dictionaryEnabled: Boolean): Unit = {
    val schemaStr =
      """
        |message root {
        |  optional INT32                    _1(INT_8);
        |  optional INT32                    _2(INT_16);
        |  optional INT32                    _3;
        |  optional INT64                    _4;
        |  optional FLOAT                    _5;
        |  optional DOUBLE                   _6;
        |  optional INT32                    _7(DECIMAL(5, 2));
        |  optional INT64                    _8(DECIMAL(18, 10));
        |  optional FIXED_LEN_BYTE_ARRAY(16) _9(DECIMAL(38, 37));
        |  optional INT64                    _10(TIMESTAMP(MILLIS,true));
        |  optional INT64                    _11(TIMESTAMP(MICROS,true));
        |  optional INT32                    _12(DATE);
        |  optional INT32                    _g1(INT_8);
        |  optional INT32                    _g2(INT_16);
        |  optional INT32                    _g3;
        |  optional INT64                    _g4;
        |  optional FLOAT                    _g5;
        |  optional DOUBLE                   _g6;
        |  optional INT32                    _g7(DECIMAL(5, 2));
        |  optional INT64                    _g8(DECIMAL(18, 10));
        |  optional FIXED_LEN_BYTE_ARRAY(16) _g9(DECIMAL(38, 37));
        |  optional INT64                    _g10(TIMESTAMP(MILLIS,true));
        |  optional INT64                    _g11(TIMESTAMP(MICROS,true));
        |  optional INT32                    _g12(DATE);
        |  optional BINARY                   _g13(UTF8);
        |  optional BINARY                   _g14;
        |}
      """.stripMargin

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(schema, path, dictionaryEnabled = true)

    val rand = scala.util.Random
    val expected = (0 until total).map { i =>
      // use a single value for the first page, to make sure dictionary encoding kicks in
      if (rand.nextBoolean()) None
      else {
        if (dictionaryEnabled) Some(i % 10) else Some(i)
      }
    }

    expected.foreach { opt =>
      val record = new SimpleGroup(schema)
      opt match {
        case Some(i) =>
          record.add(0, i.toByte)
          record.add(1, i.toShort)
          record.add(2, i)
          record.add(3, i.toLong)
          record.add(4, rand.nextFloat())
          record.add(5, rand.nextDouble())
          record.add(6, i)
          record.add(7, i.toLong)
          record.add(8, (i % 10).toString * 16)
          record.add(9, i.toLong)
          record.add(10, i.toLong)
          record.add(11, i)
          record.add(12, i.toByte % numGroups)
          record.add(13, i.toShort % numGroups)
          record.add(14, i % numGroups)
          record.add(15, i.toLong % numGroups)
          record.add(16, rand.nextFloat())
          record.add(17, rand.nextDouble())
          record.add(18, i)
          record.add(19, i.toLong)
          record.add(20, (i % 10).toString * 16)
          record.add(21, i.toLong)
          record.add(22, i.toLong)
          record.add(23, i)
          record.add(24, (i % 10).toString * 24)
          record.add(25, (i % 10).toString * 36)
        case _ =>
      }
      writer.write(record)
    }

    writer.close()
  }

  def isComplexType(dt: DataType): Boolean = dt match {
    case _: StructType | _: ArrayType | _: MapType => true
    case _ => false
  }

  protected def checkSparkAnswerWithTolerance(
                                               df: => DataFrame,
                                               absTol: Double): (SparkPlan, SparkPlan) = {
    internalCheckSparkAnswer(df, assertCometNative = false, withTol = Some(absTol))
  }

  protected def internalCheckSparkAnswer(
                                          df: => DataFrame,
                                          assertCometNative: Boolean,
                                          includeClasses: Seq[Class[_]] = Seq.empty,
                                          excludedClasses: Seq[Class[_]] = Seq.empty,
                                          withTol: Option[Double] = None): (SparkPlan, SparkPlan) = {

    var expected: Array[Row] = Array.empty
    var sparkPlan = null.asInstanceOf[SparkPlan]
    withSQLConf("spark.auron.enable" -> "false") {
      val dfSpark = datasetOfRows(spark, df.logicalPlan)
      expected = dfSpark.collect()
      sparkPlan = dfSpark.queryExecution.executedPlan
    }
    val dfComet = datasetOfRows(spark, df.logicalPlan)

    if (withTol.isDefined) {
      checkAnswerWithTolerance(dfComet, expected, withTol.get)
    } else {
      checkAnswer(dfComet, expected)
    }
    (sparkPlan, dfComet.queryExecution.executedPlan)
  }
  private def checkAnswerWithTolerance(
                                        dataFrame: DataFrame,
                                        expectedAnswer: Seq[Row],
                                        absTol: Double): Unit = {
    val actualAnswer = dataFrame.collect()
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual num rows ${actualAnswer.length} != expected num of rows ${expectedAnswer.length}")

    actualAnswer.zip(expectedAnswer).foreach { case (actualRow, expectedRow) =>
      checkAnswerRowWithTolerance(actualRow, expectedRow, absTol)
    }
  }

  /**
   * Compares two answers and makes sure the answer is within absTol of the expected result.
   */
  private def checkAnswerRowWithTolerance(
                                        actualAnswer: Row,
                                        expectedAnswer: Row,
                                        absTol: Double): Unit = {
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")
    require(absTol > 0 && absTol <= 1e-6, s"absTol $absTol is out of range (0, 1e-6]")

    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Float, expected: Float) =>
        if (actual.isInfinity || expected.isInfinity) {
          assert(actual.isInfinity == expected.isInfinity, s"actual answer $actual != $expected")
        } else if (!actual.isNaN && !expected.isNaN) {
          assert(
            math.abs(actual - expected) < absTol,
            s"actual answer $actual not within $absTol of correct answer $expected")
        }
      case (actual: Double, expected: Double) =>
        if (actual.isInfinity || expected.isInfinity) {
          assert(actual.isInfinity == expected.isInfinity, s"actual answer $actual != $expected")
        } else if (!actual.isNaN && !expected.isNaN) {
          assert(
            math.abs(actual - expected) < absTol,
            s"actual answer $actual not within $absTol of correct answer $expected")
        }
      case (actual, expected) =>
        assert(actual == expected, s"$actualAnswer did not equal $expectedAnswer")
    }
  }

  def makeDecimalRDD(num: Int, decimal: DecimalType, useDictionary: Boolean): DataFrame = {
    import testImplicits._
    val div = if (useDictionary) 5 else num // narrow the space to make it dictionary encoded
    spark
      .range(num)
      .map(_ % div)
      // Parquet doesn't allow column names with spaces, have to add an alias here.
      // Minus 500 here so that negative decimals are also tested.
      .select((($"value" - 500) / 100.0) cast decimal as Symbol("dec"))
      .coalesce(1)
  }
}
