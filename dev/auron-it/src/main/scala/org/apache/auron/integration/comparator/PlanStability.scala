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
package org.apache.auron.integration.comparator

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.commons.io.FileUtils

case class PlanCheckResult(
    generated: Boolean,
    compared: Boolean,
    stable: Boolean,
    message: Option[String] = None)

class PlanStability(
    plansDir: String,
    colSep: String = "<|COL|>",
    doubleTolerance: Double = 1e-6) {
  private def goldenPlanFile(queryId: String): File = new File(s"$plansDir/$queryId.txt")

  def generatePlanGolden(queryId: String, rawPlan: String): PlanCheckResult = {
    val file = goldenPlanFile(queryId)
    val normalized = normalizePlan(rawPlan)
    write(file, normalized)
    PlanCheckResult(generated = true, compared = false, stable = true, None)
  }

  def comparePlanGolden(queryId: String, rawPlan: String): PlanCheckResult = {
    val file = goldenPlanFile(queryId)
    if (!file.exists()) {
      return PlanCheckResult(
        generated = false,
        compared = true,
        stable = false,
        Some(s"Golden plan not found: ${file.getAbsolutePath}"))
    }
    val expected = read(file)
    val actual = normalizePlan(rawPlan)
    val ok = expected == actual
    PlanCheckResult(
      generated = false,
      compared = true,
      stable = ok,
      if (ok) None else Some(s"Plan mismatch for $queryId"))
  }

  def normalizePlan(plan: String): String = {
    val exprIdRegex = "#\\d+L?".r
    val planIdRegex = "plan_id=\\d+".r

    def indexMatches(s: String, r: Regex): Map[String, String] = {
      val m = new mutable.LinkedHashMap[String, String]()
      r.findAllMatchIn(s).map(_.toString).foreach { tok =>
        m.getOrElseUpdate(tok, (m.size + 1).toString)
      }
      m.toMap
    }
    def replace(s: String, r: Regex, m: Map[String, String], prefix: String): String = {
      r.replaceAllIn(s, mt => s"$prefix${m(mt.toString)}")
    }

    val exprMap = indexMatches(plan, exprIdRegex)
    val p1 = replace(plan, exprIdRegex, exprMap, "#")

    val pidMap = indexMatches(p1, planIdRegex)
    val p2 = replace(p1, planIdRegex, pidMap, "plan_id=")

    val p3 = p2
      .replaceAll("Arguments: [0-9]+, [0-9]+", "Arguments: X, X")
      .replaceAll("Arguments: [0-9]+", "Arguments: X")

    p3.replaceAll("""file:/[^,\s\]\)]+""", "file:/<warehouse_dir>")
  }
  private def write(file: File, content: String): Unit = {
    Option(file.getParentFile).foreach(_.mkdirs())
    FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8)
  }
  private def read(file: File): String =
    FileUtils.readFileToString(file, StandardCharsets.UTF_8)

}
