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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkQueryTestsBase
import org.apache.spark.sql.internal.SQLConf

import java.io.File

class AuronCSVReadSchemaSuite extends CSVReadSchemaSuite with SparkQueryTestsBase {}

class AuronHeaderCSVReadSchemaSuite
  extends HeaderCSVReadSchemaSuite
  with SparkQueryTestsBase {}

class AuronJsonReadSchemaSuite extends JsonReadSchemaSuite with SparkQueryTestsBase {}

class AuronOrcReadSchemaSuite extends OrcReadSchemaSuite with SparkQueryTestsBase {}

class AuronVectorizedOrcReadSchemaSuite
  extends VectorizedOrcReadSchemaSuite
  with SparkQueryTestsBase {

  import testImplicits._

  private lazy val values = 1 to 10
  private lazy val floatDF = values.map(_.toFloat).toDF("col1")
  private lazy val doubleDF = values.map(_.toDouble).toDF("col1")
  private lazy val unionDF = floatDF.union(doubleDF)

  testAuron("change column position") {
    withTempPath {
      dir =>
        withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
          val path = dir.getCanonicalPath

          val df1 = Seq(("1", "a"), ("2", "b"), ("3", "c")).toDF("col1", "col2")
          val df2 = Seq(("d", "4"), ("e", "5"), ("f", "6")).toDF("col2", "col1")
          val unionDF = df1.unionByName(df2)

          val dir1 = s"$path${File.separator}part=one"
          val dir2 = s"$path${File.separator}part=two"

          df1.write.format(format).options(options).save(dir1)
          df2.write.format(format).options(options).save(dir2)

          val df = spark.read
            .schema(unionDF.schema)
            .format(format)
            .options(options)
            .load(path)
            .select("col1", "col2")

          checkAnswer(df, unionDF)
        }
    }
  }

  testAuron("read byte, int, short, long together") {
    withTempPath {
      dir =>
        withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
          val path = dir.getCanonicalPath

          val byteDF = (Byte.MaxValue - 2 to Byte.MaxValue).map(_.toByte).toDF("col1")
          val shortDF = (Short.MaxValue - 2 to Short.MaxValue).map(_.toShort).toDF("col1")
          val intDF = (Int.MaxValue - 2 to Int.MaxValue).toDF("col1")
          val longDF = (Long.MaxValue - 2 to Long.MaxValue).toDF("col1")
          val unionDF = byteDF.union(shortDF).union(intDF).union(longDF)

          val byteDir = s"$path${File.separator}part=byte"
          val shortDir = s"$path${File.separator}part=short"
          val intDir = s"$path${File.separator}part=int"
          val longDir = s"$path${File.separator}part=long"

          byteDF.write.format(format).options(options).save(byteDir)
          shortDF.write.format(format).options(options).save(shortDir)
          intDF.write.format(format).options(options).save(intDir)
          longDF.write.format(format).options(options).save(longDir)

          val df = spark.read
            .schema(unionDF.schema)
            .format(format)
            .options(options)
            .load(path)
            .select("col1")

          checkAnswer(df, unionDF)
        }
    }
  }

  testAuron("read float and double together") {
    withTempPath {
      dir =>
        withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
          val path = dir.getCanonicalPath

          val floatDir = s"$path${File.separator}part=float"
          val doubleDir = s"$path${File.separator}part=double"

          floatDF.write.format(format).options(options).save(floatDir)
          doubleDF.write.format(format).options(options).save(doubleDir)

          val df = spark.read
            .schema(unionDF.schema)
            .format(format)
            .options(options)
            .load(path)
            .select("col1")

          checkAnswer(df, unionDF)
        }
    }
  }
}

class AuronMergedOrcReadSchemaSuite
  extends MergedOrcReadSchemaSuite
  with SparkQueryTestsBase {}

class AuronParquetReadSchemaSuite extends ParquetReadSchemaSuite with SparkQueryTestsBase {}

class AuronVectorizedParquetReadSchemaSuite
  extends VectorizedParquetReadSchemaSuite
  with SparkQueryTestsBase {}

class AuronMergedParquetReadSchemaSuite
  extends MergedParquetReadSchemaSuite
  with SparkQueryTestsBase {}
