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

import cli.ArgsParser
import org.apache.auron.integration.runner.AuronTPCDSSuite

object Main {
  def main(args: Array[String]): Unit = {
    var mainArgs = args
    if (mainArgs.length == 0) {
      mainArgs = Array[String]("--type", "tpcds", "--data-location", "dev/tpcds_1g", "--query-filter", "q1,q2,a3")
    }

    val cfg = ArgsParser.parse(mainArgs).getOrElse {
      println("参数解析失败，请检查参数格式！")
      sys.exit(1)
    }

    println(s"解析后的配置：$cfg")

    val suite: Suite = cfg.benchType.toLowerCase match {
      case "tpcds" => AuronTPCDSSuite
      case other =>
        throw new IllegalArgumentException(s"benchmark type not found: $other")
    }

    val exitCode = suite.run(cfg)
    sys.exit(exitCode)
  }
}