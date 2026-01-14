package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.{SparkQueryTestsBase, SparkTestsSharedSessionBase}
import org.apache.spark.sql.catalyst.expressions.aggregate.PercentileSuite

class AuronPercentileSuite extends PercentileSuite  with SparkTestsSharedSessionBase