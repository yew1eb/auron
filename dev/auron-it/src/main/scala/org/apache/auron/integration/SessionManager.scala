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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SessionManager(val extraSparkConf: Map[String, String]) {

  private def resolveMaster(fallback: String = "local[4]"): String = {
    val sysProp = sys.props.get("spark.master").filter(_.nonEmpty)
    sysProp.getOrElse(fallback)
  }

  def commonConf(master: String): SparkConf = {
    new SparkConf()
      .setMaster(master)
      .set("spark.ui.enabled", "true")
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
  def baselineConf(master: String): SparkConf = {
    val conf = commonConf(master)
      .setAppName("AuronBaseline")
    conf
  }
  def auronConf(master: String): SparkConf = {
    val conf = commonConf(master)
      .setAppName("AuronTest")
      .set("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.auron.enable", "true")

    extraSparkConf.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  lazy val baselineSpark: SparkSession = {
    val master = resolveMaster()
    val conf = baselineConf(master)
    SparkSession.builder().config(conf).getOrCreate()
  }

  lazy val auronSpark: SparkSession = {
    val master = resolveMaster()
    val conf = auronConf(master)
    SparkSession.builder().config(conf).getOrCreate()
  }

  def stopAll(): Unit = {
    baselineSpark.stop()
    auronSpark.stop()
  }
}
