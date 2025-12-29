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

import org.apache.spark.sql.auron.Shims
import scopt.OParser

import org.apache.auron.integration.runner.AuronTPCDSSuite

object Main {
  val parser = {
    val builder = OParser.builder[SuiteArgs]
    import builder._
    OParser.sequence(
      programName("auron-it"),
      head("auron-it", "v1.0"),
      opt[String]('t', "type")
        .action((x, c) => c.copy(benchType = x))
        .required()
        .text("benchmark type (tpcds)"),
      opt[String]('d', "data-location")
        .action((x, c) => c.copy(dataLocation = x))
        .required()
        .text("data directory path"),
      opt[String]('q', "query-filter")
        .action((x, c) => c.copy(queryFilter = x.split(",").map(_.trim).filter(_.nonEmpty).toSeq))
        //.required()
        .text("query filter (e.g. q1,q2,q3)"),
      opt[String]("conf")
        .unbounded()
        .valueName("k=v")
        .action { (x, c) =>
          val Array(k, v) = x.split("=", 2)
          c.copy(extraSparkConf = c.extraSparkConf + (k -> v))
        }
        .validate { x =>
          if (x.contains("=")) Right(()) else Left(s"--conf expects k=v, got: $x")
        }
        .text("Spark configuration, repeatable: --conf k=v --conf a=b"),
      opt[Unit]("plan-check")
        .action((_, c) => c.copy(enablePlanCheck = true))
        .text("enable plan stability check(default: false)"),
      opt[Unit]("regen-golden")
        .action((_, c) => c.copy(regenGoldenFiles = true))
        .text("regenerate golden files"),
      opt[Unit]("disable-result-check")
        .action((_, c) => c.copy(disableResultCheck = true))
        .text("disable query result check (default: enabled)"),
      help('h', "help"))
  }

  def parseArgs(args: Array[String]): Option[SuiteArgs] = {
    OParser.parse(parser, args, SuiteArgs())
  }

  def main(mainArgs: Array[String]): Unit = {
    parseArgs(mainArgs) match {
      case Some(args) =>
        println(s"""
                   |Auron Integration Test (type: ${args.benchType})
                   |Spark Version: ${Shims.get.shimVersion}
                   |Data: ${args.dataLocation}
                   |Queries: [${args.queryFilter
          .mkString(", ")}] (${args.queryFilter.length} queries)
                   |Extra Spark Conf: ${args.extraSparkConf.mkString("", "; ", "")}
                   |Plan Check: ${if (args.enablePlanCheck) "Enabled" else "Disabled"}
                   |Regen Golden Files: ${if (args.regenGoldenFiles) "Yes" else "No"}
          """.stripMargin)

        val suite = createSuite(args)
        var exitCode = 0
        try {
          exitCode = suite.run()
        } finally {
          suite.close()
        }

        if (exitCode != 0) {
          sys.exit(exitCode)
        }
      case None =>
        sys.exit(1)
    }
  }

  private def createSuite(args: SuiteArgs): Suite = args.benchType match {
    case "tpcds" => AuronTPCDSSuite(args)
    case other =>
      println(s"Unsupported benchmark type: $other")
      sys.exit(1)
  }
}
