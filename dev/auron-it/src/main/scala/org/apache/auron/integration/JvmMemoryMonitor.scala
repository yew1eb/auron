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

import java.lang.management.ManagementFactory
import java.text.SimpleDateFormat
import java.util.{Date, Timer, TimerTask}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.Try

// scalastyle:off
object JvmMemoryMonitor {
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def start(): Unit = {
    val timer = new Timer("JVM-Memory-Monitor", true)
    timer.scheduleAtFixedRate(
      new TimerTask {
        override def run(): Unit = printMemoryInfo()
      },
      0,
      2000)
  }

  private def printMemoryInfo(): Unit = {
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime()

    val jvmUsed = (runtime.totalMemory() - runtime.freeMemory()) / mb
    val jvmMax = runtime.maxMemory() / mb

    val mxBean = ManagementFactory.getMemoryMXBean()
    val heapUsed = mxBean.getHeapMemoryUsage().getUsed() / mb
    val nonHeapUsed = mxBean.getNonHeapMemoryUsage().getUsed() / mb

    val directUsed = ManagementFactory.getMemoryPoolMXBeans
      .filter(_.getName.contains("Direct"))
      .map(_.getUsage.getUsed / mb)
      .headOption
      .getOrElse(0L)

    val memInfo = Try {
      scala.io.Source
        .fromFile("/proc/meminfo")
        .getLines()
        .map(_.split("\\s+"))
        .filter(arr => arr(0) == "MemTotal:" || arr(0) == "MemUsed:" || arr(0) == "MemAvailable:")
        .map(arr => (arr(0).replace(":", ""), arr(1).toLong / 1024))
        .toMap
    }.getOrElse(Map.empty[String, Long])

    val sysTotal = memInfo.getOrElse("MemTotal", 0L)
    val sysUsed = sysTotal - memInfo.getOrElse("MemAvailable", 0L)

    val currentTime = dateFormat.format(new Date())

    println(s"[$currentTime]")
    println(s"\n=== Memory Monitor ===")
    println(
      s"JVM: Used=$jvmUsed MB (Max=$jvmMax MB) | Heap=$heapUsed MB | Non-Heap=$nonHeapUsed MB | Off-Heap=$directUsed MB")
    println(s"OS (Ubuntu): Used=$sysUsed MB (Total=$sysTotal MB)")
    println("----------------------------------------")
  }
  // scalastyle:on
}
