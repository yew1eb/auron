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
package org.apache.auron.integration.exec

import org.apache.spark.sql.SparkSession
object SparkFactory {
  def create(withGluten: Boolean): SparkSession = {
    val builder = SparkSession.builder()
      .appName(if (withGluten) "Auron-Gluten" else "Auron-Spark")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
    if (withGluten) {
      builder
        .config("spark.plugins", "org.apache.spark.sql.extension.GlutenPlugin")
        .config("spark.sql.columnar.enabled", "true")
        .config("spark.gluten.enabled", "true")
      // 如需后端配置，在此追加
    } else {
      builder
        .config("spark.plugins", "")
        .config("spark.sql.columnar.enabled", "false")
        .config("spark.gluten.enabled", "false")
    }
    builder.getOrCreate()
  }
}