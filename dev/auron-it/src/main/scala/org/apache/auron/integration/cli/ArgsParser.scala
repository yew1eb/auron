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
package org.apache.auron.integration.cli

import scopt.OParser

object ArgsParser {


  case class Config(
                     benchType: String = "tpcds",
                     dataLocation: String = "",
                     queryFilter: Seq[String] = Seq.empty
                   )

  // 构建命令行解析器（单例，避免重复创建）
  private val parser: OParser[Unit, Config] = {
    val builder = OParser.builder[Config]
    import builder._

    OParser.sequence(
      programName("AuronRunner"),
      head("AuronRunner", "1.0"),
      note("\n使用示例: AuronRunner --type tpcds --data-location dev/tpcds_1g --query-filter q1,q2,a3"),


      opt[String]("type")
        .optional()
        .valueName("<bench-type>")
        .text(s"基准测试类型，默认值: tpcds")
        .action((value, config) => config.copy(benchType = value)),

      opt[String]("data-location")
        .required()
        .valueName("<path>")
        .text("数据存储位置（必填）")
        .validate(value =>
          if (value.nonEmpty) Right(())
          else Left("数据存储位置不能为空")
        )
        .action((value, config) => config.copy(dataLocation = value)),

      opt[Seq[String]]("query-filter")
        .optional()
        .valueName("<queries>")
        .text("查询过滤器列表，多个值用逗号分隔")
        .action((value, config) => config.copy(queryFilter = value)),

      // 帮助信息
      help("help").text("显示帮助信息")
    )
  }


  def parse(args: Array[String]): Option[Config] = {
    OParser.parse(parser, args, Config())
  }
}