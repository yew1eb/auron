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
package org.apache.auron.integration

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class MainTest extends AnyFunSuite with Matchers {
  protected val tpcdsDataPath: String =
    sys.env.getOrElse("SPARK_TPCDS_DATA", "/Users/yew1eb/workspaces/tpcds-validator/tpcds_1g")

  test("parse mainArgs") {
    val args = Array[String](
      "--type",
      "tpcds",
      "--data-location",
      "dev/tpcds_1g",
      "--query-filter",
      "q1,q2,a3",
      "--conf",
      "spark.serializer=org.apache.spark.serializer.KryoSerializer",
      "--conf",
      "spark.celeborn.client.spark.shuffle.writer=hash")
    Main.parseArgs(args) match {
      case Some(conf) =>
        println(conf)
      case None =>
        fail(s"failed parse args: ${args.mkString("Array(", ", ", ")")}")
    }
  }

  test("query result check") {
    assume(tpcdsDataPath.nonEmpty, "Skip: SPARK_TPCDS_DATA env not set")
    val args = Array[String](
      "--type",
      "tpcds",
      "--data-location",
      tpcdsDataPath,
      "--query-filter",
      "q1,q2,q3,q4,q5,q6")
    Main.main(args)
  }

  test("plan check") {
    assume(tpcdsDataPath.nonEmpty, "Skip: SPARK_TPCDS_DATA env not set")
    val args = Array[String](
      "--type",
      "tpcds",
      "--data-location",
      tpcdsDataPath,
      "--query-filter",
      "q1",
      "--plan-check")
    Main.main(args)
  }

  test("regen golden files") {
    assume(tpcdsDataPath.nonEmpty, "Skip: SPARK_TPCDS_DATA env not set")
    val args = Array[String](
      "--type",
      "tpcds",
      "--data-location",
      tpcdsDataPath,
      "--query-filter",
      "q1,q8",
      "--regen-golden")
    Main.main(args)
  }

  test("check all") {
    assume(tpcdsDataPath.nonEmpty, "Skip: SPARK_TPCDS_DATA env not set")
    val args = Array[String]("--type", "tpcds", "--data-location", tpcdsDataPath)
    Main.main(args)
  }
}
