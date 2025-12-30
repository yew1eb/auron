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

class SessionManager(val extraSparkConfMap: Map[String, String]) {

  private var _spark: SparkSession = _
  private var _name: String = ""

  private def resolveMaster(fallback: String = "local[4]"): String = {
    val sysProp = sys.props.get("spark.master").filter(_.nonEmpty)
    sysProp.getOrElse(fallback)
  }

  private lazy val commonConfMap: Map[String, String] = Map(
    "spark.master" -> resolveMaster(),
    "spark.sql.shuffle.partitions" -> "100",
    "spark.ui.enabled" -> "true",
    "spark.sql.sources.useV1SourceList" -> "parquet",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1")

  private lazy val auronConfMap: Map[String, String] =
    commonConfMap ++ Map(
      "spark.sql.extensions" -> "org.apache.spark.sql.auron.AuronSparkSessionExtension",
      "spark.shuffle.manager" -> "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager",
      "spark.auron.native.log.level" -> "warn",
      "spark.auron.enable" -> "true") ++ extraSparkConfMap

  private lazy val confsMap: Map[String, Map[String, String]] =
    Map("baseline" -> commonConfMap, "auron" -> auronConfMap)

  def baselineSession: SparkSession = synchronized {
    getOrCreateSession(name = "baseline", appName = "baseline-app")
  }

  def auronSession: SparkSession = synchronized {
    getOrCreateSession(name = "auron", appName = "auron-app")
  }

  private def getOrCreateSession(name: String, appName: String): SparkSession = synchronized {
    if (_name == name && _spark != null) {
      return _spark
    }

    stopActiveSession()
    val sparkConf = new SparkConf()
    confsMap(name).foreach { case (k, v) => sparkConf.set(k, v) }

    activateSession(sparkConf, appName)
    _name = name
    println(s"Successfully getOrCreateSession $name.")
    _spark
  }

  private def stopActiveSession(): Unit = synchronized {
    try {
      if (_spark != null) {
        try {
          _spark.sessionState.catalog.reset()
        } finally {
          _spark.stop()
          _spark = null
          _name = ""
        }
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def activateSession(conf: SparkConf, appName: String): Unit = {
    if (_spark != null) {
      stopActiveSession()
    }
    createSession(conf, appName = appName)
    SparkSession.setDefaultSession(_spark)
    SparkSession.setActiveSession(_spark)
  }

  private def createSession(conf: SparkConf, appName: String): Unit = {
    if (_spark != null) {
      throw new IllegalStateException()
    }
    _spark = SparkSession.builder().config(conf).getOrCreate()
    _spark.sparkContext.setLogLevel("WARN")
  }

  def close(): Unit = {
    stopActiveSession()
  }
}
