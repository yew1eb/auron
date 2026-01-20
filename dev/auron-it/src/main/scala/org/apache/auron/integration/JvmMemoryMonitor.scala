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

// scalastyle:off
object JvmMemoryMonitor {
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def start(): Unit = {
    val timer = new Timer("JVM-Memory-Monitor", true)
    timer.scheduleAtFixedRate(
      new TimerTask {
        override def run(): Unit = printJvmMemoryInfo()
      },
      0,
      2000)
  }

  def printJvmMemoryInfo(): Unit = {
    val runtime = Runtime.getRuntime()
    val mb = 1024 * 1024

    // Basic memory metrics (MB)
    val totalMemory = runtime.totalMemory() / mb // Total memory allocated to JVM
    val freeMemory = runtime.freeMemory() / mb // Free memory in JVM
    val usedMemory = totalMemory - freeMemory // Used memory in JVM
    val maxMemory = runtime.maxMemory() / mb // Maximum memory JVM can use
    val memoryUsagePercent = (usedMemory.toDouble / maxMemory * 100).formatted("%.2f")

    // Heap and Non-Heap memory details (more accurate)
    val memoryMXBean = ManagementFactory.getMemoryMXBean()
    val heapUsed = memoryMXBean.getHeapMemoryUsage().getUsed() / mb
    val nonHeapUsed = memoryMXBean.getNonHeapMemoryUsage().getUsed() / mb

    // Get current timestamp
    val currentTime = dateFormat.format(new Date())

    // Print formatted memory info
    println(s"[$currentTime]")
    println(s"\n=== JVM Memory Monitor ===")
    println(
      s"Total Memory: $totalMemory MB | Used Memory: $usedMemory MB | Free Memory: $freeMemory MB")
    println(s"Max Memory: $maxMemory MB | Memory Usage: $memoryUsagePercent%")
    println(s"Heap Memory Used: $heapUsed MB | Non-Heap Memory Used: $nonHeapUsed MB")
    println("============================================")
  }
  // scalastyle:on
}
