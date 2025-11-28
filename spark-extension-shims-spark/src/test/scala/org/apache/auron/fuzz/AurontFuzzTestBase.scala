package org.apache.auron.fuzz

import org.apache.auron.BaseAuronSQLSuite
import org.apache.auron.testing.{DataGenOptions, ParquetGenerator, SchemaGenOptions}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.auron.plan.{NativeParquetScanExec, NativeShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.scalactic.source.Position
import org.scalatest.Tag

import java.io.File
import java.text.SimpleDateFormat
import scala.util.Random

class AurontFuzzTestBase extends AuronQueryTest with BaseAuronSQLSuite {

  var filename: String = null

  /**
   * We use Asia/Kathmandu because it has a non-zero number of minutes as the offset, so is an
   * interesting edge case. Also, this timezone tends to be different from the default system
   * timezone.
   *
   * Represents UTC+5:45
   */
  val defaultTimezone = "Asia/Kathmandu"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tempDir = System.getProperty("java.io.tmpdir")
    filename = s"$tempDir/CometFuzzTestSuite_${System.currentTimeMillis()}.parquet"
    val random = new Random(42)
    withSQLConf(
      "spark.auron.enable"  -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {
      val schemaGenOptions =
        SchemaGenOptions(generateArray = true, generateStruct = true)
      val dataGenOptions = DataGenOptions(
        generateNegativeZero = false,
        // override base date due to known issues with experimental scans
        baseDate =
          new SimpleDateFormat("YYYY-MM-DD hh:mm:ss").parse("2024-05-25 12:34:56").getTime)
      ParquetGenerator.makeParquetFile(
        random,
        spark,
        filename,
        1000,
        schemaGenOptions,
        dataGenOptions)
    }
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteDirectory(new File(filename))
  }

  def collectNativeScans(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) {
      case scan: NativeParquetScanExec => scan
    }
  }

  def collectCometShuffleExchanges(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) { case exchange: NativeShuffleExchangeExec =>
      exchange
    }
  }

}
