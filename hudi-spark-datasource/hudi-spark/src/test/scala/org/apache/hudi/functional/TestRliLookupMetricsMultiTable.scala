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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.config.metrics.HoodieMetricsConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.metrics.{ExecutorMetricRegistry, MetricsReporterType, RecordIndexMetricNames}
import org.apache.hudi.testutils.CapturingMetricsReporter

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.JavaConverters._

/** Two tables written from one JVM must not see each other's counters. */
@Tag("functional")
class TestRliLookupMetricsMultiTable extends RecordLevelIndexTestBase {

  private val tableA = "rli_multi_a"
  private val tableB = "rli_multi_b"

  /** The reporter is process-wide, so each test starts from a clean slate. */
  @org.junit.jupiter.api.BeforeEach
  def resetCapturedMetrics(): Unit = CapturingMetricsReporter.reset()

  private def pathFor(table: String): String = s"$basePath/$table"

  private def optsFor(table: String): Map[String, String] = Map(
    RECORDKEY_FIELD.key -> "key",
    PARTITIONPATH_FIELD.key -> "part",
    PRECOMBINE_FIELD.key -> "ts",
    HoodieWriteConfig.TBL_NAME.key -> table,
    TABLE_TYPE.key -> COW_TABLE_TYPE_OPT_VAL,
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false",
    HoodieIndexConfig.INDEX_TYPE.key -> "GLOBAL_RECORD_LEVEL_INDEX",
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    HoodieMetricsConfig.TURN_METRICS_ON.key -> "true",
    HoodieMetricsConfig.RLI_LOOKUP_METRICS_ENABLE.key -> "true",
    HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key -> MetricsReporterType.INMEMORY.name(),
    HoodieMetricsConfig.METRICS_REPORTER_CLASS_NAME.key -> classOf[CapturingMetricsReporter].getName,
    // Named explicitly rather than left to be inferred from the table name: it is what separates the
    // two tables' gauges in the shared reporter.
    HoodieMetricsConfig.METRICS_REPORTER_PREFIX.key -> table)

  /** Keys are namespaced per table so a cross-table leak cannot accidentally look like a correct hit. */
  private def write(table: String, operation: String, saveMode: SaveMode, from: Int, until: Int): Unit = {
    val schema = StructType(Seq(
      StructField("key", StringType), StructField("part", StringType), StructField("ts", LongType)))
    val rows = (from until until).map(i => Row(s"$table-key-$i", s"p${i % 3}", i.toLong))
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
      .write.format("hudi").options(optsFor(table))
      .option(OPERATION.key, operation)
      .mode(saveMode).save(pathFor(table))
  }

  /**
   * Read back the way an operator would, off the reporter. The gauge name carries the per-table prefix,
   * so a leak between tables shows up as a count landing under the wrong name rather than going unseen.
   */
  private def countersOn(table: String): Map[String, String] = {
    val prefix = s"$table.${ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricAction()}." +
      s"${ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricQualifier()}."
    CapturingMetricsReporter.captured().asScala.toMap
      .collect { case (name, value) if name.startsWith(prefix) =>
        name.substring(prefix.length) -> value.toString }
  }

  /**
   * A caller that looked something up stamps its full counter set, zeros included, so the default only
   * covers a caller that contributed nothing at all.
   */
  private def tagCount(counters: Map[String, String], metric: String): Long =
    counters.getOrElse(metric, "0").toLong

  private def report(label: String, counters: Map[String, String]): Unit = {
    println(s"\n===== $label =====")
    counters.toSeq.sorted.foreach { case (k, v) => println(f"  $k%-52s $v") }
    println("=" * (12 + label.length) + "\n")
  }

  @Test
  def testTwoTablesInOneJvmDoNotShareCounters(): Unit = {
    // Seed both tables. Inserts do not tag, so no counters are produced yet.
    write(tableA, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, 0, 100)
    write(tableB, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, 0, 100)

    // Interleaved upserts of deliberately different sizes. If the registries were shared, B's commit
    // would carry A's 30 as well -- or A's would be drained by B before A ever committed.
    write(tableA, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, 0, 30)
    write(tableB, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, 0, 7)
    write(tableA, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, 0, 12)

    val aCounters = countersOn(tableA)
    val bCounters = countersOn(tableB)
    report(s"table A ($tableA) -- latest commit, expected 12", aCounters)
    report(s"table B ($tableB) -- latest commit, expected 7", bCounters)

    assertTrue(aCounters.nonEmpty, "table A must carry its own counters")
    assertTrue(bCounters.nonEmpty, "table B must carry its own counters")

    assertEquals(12L, tagCount(aCounters, RecordIndexMetricNames.KEY_COUNT),
      "table A's latest commit must report only its own 12 keys")
    assertEquals(12L, tagCount(aCounters, RecordIndexMetricNames.KEY_HIT_COUNT),
      "all 12 of A's keys already exist in A's index")

    assertEquals(7L, tagCount(bCounters, RecordIndexMetricNames.KEY_COUNT),
      "table B's latest commit must report only its own 7 keys, not A's interleaved 30 or 12")
    assertEquals(7L, tagCount(bCounters, RecordIndexMetricNames.KEY_HIT_COUNT),
      "all 7 of B's keys already exist in B's index")

    assertEquals(0L, tagCount(aCounters, RecordIndexMetricNames.KEY_MISS_COUNT), "A upserted only existing keys")
    assertEquals(0L, tagCount(bCounters, RecordIndexMetricNames.KEY_MISS_COUNT), "B upserted only existing keys")
  }

  /** A table written after another has finished must start from zero, not inherit a residue. */
  @Test
  def testSecondTableStartsFromZero(): Unit = {
    write(tableA, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, 0, 60)
    write(tableA, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, 0, 25)
    assertEquals(25L, tagCount(countersOn(tableA), RecordIndexMetricNames.KEY_COUNT))

    write(tableB, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, 0, 60)
    write(tableB, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, 0, 9)

    val bCounters = countersOn(tableB)
    report(s"table B after A completed -- expected 9", bCounters)
    assertEquals(9L, tagCount(bCounters, RecordIndexMetricNames.KEY_COUNT),
      "a table written after another finished must not inherit its counters")
  }
}
