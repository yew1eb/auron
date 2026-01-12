package org.apache.auron.test

import org.apache.spark.sql.execution.SparkPlan

object FallbackUtil {





  def hasFallback(plan: SparkPlan): Boolean = {
    println(s"[WARN] ${plan}")
    true
//    val fallbackOperator =
//      collectWithSubqueries(plan) { case plan => plan }.filter(plan => nodeHasFallback(plan))
//    fallbackOperator.foreach(operator => log.info(s"gluten fallback operator:{$operator}"))
//    fallbackOperator.nonEmpty
  }

}
