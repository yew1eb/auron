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
package org.apache.spark.sql.excution.joins

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkQueryTestsBase, SparkSession, SparkTestsSharedSessionBase}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.execution.joins.BroadcastJoinSuite
import org.apache.spark.sql.internal.SQLConf

/**
 * This test needs setting for spark test home (its source code), e.g., appending the following
 * setting for `mvn test`: -DargLine="-Dspark.test.home=/home/sparkuser/spark/".
 *
 * In addition, you also need build spark source code before running this test, e.g., with
 * `./build/mvn -DskipTests clean package`.
 */
// 最终类：继承间接父类，无需直接接触BroadcastJoinSuite的权限问题
class AuronBroadcastJoinSuite extends BroadcastJoinSuiteWrapper {
  // 你的业务逻辑代码
}

// 间接父类：中转权限，包装BroadcastJoinSuite功能
abstract class BroadcastJoinSuiteWrapper extends SparkTestsSharedSessionBase {
  // 持有BroadcastJoinSuite实例，保留其功能
  protected val broadcastJoinCore = new BroadcastJoinSuite()

  // 暴露public权限的核心成员，与SparkTestsSharedSessionBase兼容
  override def beforeAll(): Unit = {
    super.beforeAll()
    broadcastJoinCore.beforeAll() // 若直接调用报错，替换为方案2的反射调用
  }

  override def afterAll(): Unit = {
    broadcastJoinCore.afterAll()
    super.afterAll()
  }

  override val spark: SparkSession = {
    val session = super.spark
    // 传递spark会话给BroadcastJoinSuite（无公开方法则用反射）
    session
  }
}

