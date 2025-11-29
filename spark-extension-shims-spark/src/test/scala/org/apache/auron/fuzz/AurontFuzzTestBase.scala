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

import java.io.File
import java.text.SimpleDateFormat

import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.auron.plan.{NativeParquetScanExec, NativeShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.auron.BaseAuronSQLSuite
import org.apache.auron.testing.{DataGenOptions, ParquetGenerator, SchemaGenOptions}

class AurontFuzzTestBase extends AuronQueryTest with BaseAuronSQLSuite {

  var filename: String = null

  /**
   * We use Asia/Kathmandu because it has a non-zero number of minutes as the offset, so is an
   * interesting edge case. Also, this timezone tends to be different from the default system
   * timezone.
   *
   * Represents UTC+5:45
   */
  val defaultTimezone = "Asia/Kathmandu"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tempDir = System.getProperty("java.io.tmpdir")
    filename = s"$tempDir/CometFuzzTestSuite_${System.currentTimeMillis()}.parquet"
    val random = new Random(42)
    withSQLConf(
      "spark.auron.enable" -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {
      val schemaGenOptions =
        SchemaGenOptions(generateArray = true, generateStruct = true)
      val dataGenOptions = DataGenOptions(
        generateNegativeZero = false,
        // override base date due to known issues with experimental scans
        baseDate =
          new SimpleDateFormat("YYYY-MM-DD hh:mm:ss").parse("2024-05-25 12:34:56").getTime)
      ParquetGenerator.makeParquetFile(
        random,
        spark,
        filename,
        1000,
        schemaGenOptions,
        dataGenOptions)
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteDirectory(new File(filename))
  }

  def collectNativeScans(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) { case scan: NativeParquetScanExec =>
      scan
    }
  }

  def collectCometShuffleExchanges(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) { case exchange: NativeShuffleExchangeExec =>
      exchange
    }
  }

  /**
   {
   "type" : "struct",
   "fields" : [ {
   "name" : "c0",
   "type" : "boolean",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c1",
   "type" : "byte",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c2",
   "type" : "short",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c3",
   "type" : "integer",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c4",
   "type" : "long",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c5",
   "type" : "float",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c6",
   "type" : "double",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c7",
   "type" : "decimal(10,2)",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c8",
   "type" : "decimal(36,18)",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c9",
   "type" : "date",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c10",
   "type" : "timestamp",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c11",
   "type" : "timestamp_ntz",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c12",
   "type" : "string",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c13",
   "type" : "binary",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c14",
   "type" : {
   "type" : "struct",
   "fields" : [ {
   "name" : "c0",
   "type" : "boolean",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c1",
   "type" : "byte",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c2",
   "type" : "short",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c3",
   "type" : "integer",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c4",
   "type" : "long",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c5",
   "type" : "float",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c6",
   "type" : "double",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c7",
   "type" : "decimal(10,2)",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c8",
   "type" : "decimal(36,18)",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c9",
   "type" : "date",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c10",
   "type" : "timestamp",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c11",
   "type" : "timestamp_ntz",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c12",
   "type" : "string",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c13",
   "type" : "binary",
   "nullable" : true,
   "metadata" : { }
   } ]
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c15",
   "type" : {
   "type" : "struct",
   "fields" : [ {
   "name" : "c0",
   "type" : {
   "type" : "array",
   "elementType" : "boolean",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c1",
   "type" : {
   "type" : "array",
   "elementType" : "byte",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c2",
   "type" : {
   "type" : "array",
   "elementType" : "short",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c3",
   "type" : {
   "type" : "array",
   "elementType" : "integer",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c4",
   "type" : {
   "type" : "array",
   "elementType" : "long",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c5",
   "type" : {
   "type" : "array",
   "elementType" : "float",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c6",
   "type" : {
   "type" : "array",
   "elementType" : "double",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c7",
   "type" : {
   "type" : "array",
   "elementType" : "decimal(10,2)",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c8",
   "type" : {
   "type" : "array",
   "elementType" : "decimal(36,18)",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c9",
   "type" : {
   "type" : "array",
   "elementType" : "date",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c10",
   "type" : {
   "type" : "array",
   "elementType" : "timestamp",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c11",
   "type" : {
   "type" : "array",
   "elementType" : "timestamp_ntz",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c12",
   "type" : {
   "type" : "array",
   "elementType" : "string",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c13",
   "type" : {
   "type" : "array",
   "elementType" : "binary",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   } ]
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c16",
   "type" : {
   "type" : "array",
   "elementType" : "boolean",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c17",
   "type" : {
   "type" : "array",
   "elementType" : "byte",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c18",
   "type" : {
   "type" : "array",
   "elementType" : "short",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c19",
   "type" : {
   "type" : "array",
   "elementType" : "integer",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c20",
   "type" : {
   "type" : "array",
   "elementType" : "long",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c21",
   "type" : {
   "type" : "array",
   "elementType" : "float",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c22",
   "type" : {
   "type" : "array",
   "elementType" : "double",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c23",
   "type" : {
   "type" : "array",
   "elementType" : "decimal(10,2)",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c24",
   "type" : {
   "type" : "array",
   "elementType" : "decimal(36,18)",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c25",
   "type" : {
   "type" : "array",
   "elementType" : "date",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c26",
   "type" : {
   "type" : "array",
   "elementType" : "timestamp",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c27",
   "type" : {
   "type" : "array",
   "elementType" : "timestamp_ntz",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c28",
   "type" : {
   "type" : "array",
   "elementType" : "string",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c29",
   "type" : {
   "type" : "array",
   "elementType" : "binary",
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c30",
   "type" : {
   "type" : "array",
   "elementType" : {
   "type" : "struct",
   "fields" : [ {
   "name" : "c0",
   "type" : "boolean",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c1",
   "type" : "byte",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c2",
   "type" : "short",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c3",
   "type" : "integer",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c4",
   "type" : "long",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c5",
   "type" : "float",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c6",
   "type" : "double",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c7",
   "type" : "decimal(10,2)",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c8",
   "type" : "decimal(36,18)",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c9",
   "type" : "date",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c10",
   "type" : "timestamp",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c11",
   "type" : "timestamp_ntz",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c12",
   "type" : "string",
   "nullable" : true,
   "metadata" : { }
   }, {
   "name" : "c13",
   "type" : "binary",
   "nullable" : true,
   "metadata" : { }
   } ]
   },
   "containsNull" : true
   },
   "nullable" : true,
   "metadata" : { }
   } ]
   }
   */
}
