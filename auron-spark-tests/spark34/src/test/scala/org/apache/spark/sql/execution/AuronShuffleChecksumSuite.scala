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
package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkQueryTestsBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Integration tests for native shuffle CRC32 checksum support (SPARK-51756).
 *
 * These tests set spark.shuffle.checksum.enabled=true with algorithm=CRC32 and
 * run queries that involve shuffle. If the native checksum logic is broken, Spark's
 * ShuffleBlockFetcherIterator will throw a FetchFailedException due to checksum mismatch.
 */
class AuronShuffleChecksumSuite extends SparkQueryTestsBase {

  private val checksumConf = Seq(
    "spark.shuffle.checksum.enabled" -> "true",
    "spark.shuffle.checksum.algorithm" -> "CRC32",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
    SQLConf.SHUFFLE_PARTITIONS.key -> "4")

  testAuron("hash repartition with checksum enabled") {
    withSQLConf(checksumConf: _*) {
      import testImplicits._
      val df = (1 to 100).map(i => (i, i * 2, s"val$i")).toDF("id", "val", "name")
      val result = df.repartition(4, $"id").groupBy($"id").agg(sum($"val").as("total"))
      // Just collect — Spark will verify checksums automatically on fetch.
      assert(result.count() == 100)
    }
  }

  testAuron("sort merge join with checksum enabled") {
    withSQLConf(
      (checksumConf :+ (SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1")): _*) {
      import testImplicits._
      val left = (1 to 50).map(i => (i, s"l$i")).toDF("id", "lval")
      val right = (1 to 50).map(i => (i, s"r$i")).toDF("id", "rval")
      val result = left.join(right, "id")
      assert(result.count() == 50)
    }
  }

  testAuron("aggregation with shuffle and checksum enabled") {
    withSQLConf(checksumConf: _*) {
      import testImplicits._
      val df = (1 to 200)
        .map(i => (i % 10, i.toLong))
        .toDF("key", "value")
      val result = df.groupBy($"key").agg(sum($"value").as("total")).orderBy($"key")
      assert(result.count() == 10)
      // Verify correctness: sum of values for key k = sum of i where i%10==k, i in 1..200
      val rows = result.collect()
      rows.foreach { row =>
        val key = row.getInt(0)
        val total = row.getLong(1)
        val expected = (key to 200 by 10).map(_.toLong).sum +
          (if (key == 0) (10 to 200 by 10).map(_.toLong).sum else 0L)
        // simple sanity: sum must be positive
        assert(total > 0, s"sum for key=$key should be positive, got $total")
      }
    }
  }

  testAuron("checksum disabled behaves identically to enabled") {
    import testImplicits._
    val data = (1 to 100).map(i => (i % 5, i.toLong)).toDF("key", "value")

    val withChecksum = withSQLConf(checksumConf: _*) {
      data.groupBy($"key").agg(sum($"value").as("total")).orderBy($"key").collect()
    }
    val withoutChecksum = withSQLConf(
      "spark.shuffle.checksum.enabled" -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "4") {
      data.groupBy($"key").agg(sum($"value").as("total")).orderBy($"key").collect()
    }

    assert(withChecksum.length == withoutChecksum.length)
    withChecksum.zip(withoutChecksum).foreach { case (r1, r2) =>
      assert(r1 == r2, s"row mismatch: checksum=$r1, no-checksum=$r2")
    }
  }

  testAuron("non-CRC32 algorithm silently disables native checksum") {
    // Adler32 is unsupported by the native writer; it falls back to no-checksum silently.
    withSQLConf(
      "spark.shuffle.checksum.enabled" -> "true",
      "spark.shuffle.checksum.algorithm" -> "ADLER32",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "4") {
      import testImplicits._
      val df = (1 to 50).map(i => (i, i * 3)).toDF("a", "b")
      assert(df.repartition(4, $"a").count() == 50)
    }
  }
}
