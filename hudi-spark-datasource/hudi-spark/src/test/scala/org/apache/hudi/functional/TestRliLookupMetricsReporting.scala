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
import org.apache.hudi.common.config.metrics.HoodieMetricsConfig
import org.apache.hudi.metrics.{ExecutorMetricRegistry, RecordIndexMetricNames}

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import java.io.{ByteArrayOutputStream, PrintStream}

/** The counters must reach a live metrics reporter, not only commit metadata. */
@Tag("functional")
class TestRliLookupMetricsReporting extends RliLookupMetricsTestBase {

  private def metricsOpts: Map[String, String] = rliOpts ++ Map(
    HoodieMetricsConfig.TURN_METRICS_ON.key -> "true",
    HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key -> "CONSOLE")

  /** Captures stdout for the duration of the write. */
  private def captureStdout(body: => Unit): String = {
    val buffer = new ByteArrayOutputStream()
    val original = System.out
    try {
      System.setOut(new PrintStream(buffer, true, "UTF-8"))
      body
    } finally {
      System.setOut(original)
    }
    buffer.toString("UTF-8")
  }

  @Test
  def testCountersReachBothCommitMetadataAndTheReporter(): Unit = {
    val numUpdates = 12

    doWriteAndValidateDataAndRecordIndex(metricsOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite,
      validate = false, numInserts = 50)
    clearRliRegistry()

    val stdout = captureStdout {
      doWriteAndValidateDataAndRecordIndex(metricsOpts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append,
        validate = false, numUpdates = numUpdates)
    }

    // Sink 1 -- the timeline.
    val counters = rliCountersFromLatestCommit()
    report(s"Reporter test ($indexLabel) -- commit metadata", counters)
    assertTrue(counters.nonEmpty, "the commit must still carry the counters when a reporter is configured")
    assertEquals(numUpdates.toString, counters(tagKey(RecordIndexMetricNames.KEY_HIT_COUNT)))
    assertEquals((numUpdates + 1).toLong, assertSumInvariant(counters, RecordIndexMetricNames.CALLER_TAG_LOCATION))

    // Sink 2 -- the reporter. Gauge names are <prefix>.rli.lookup.<caller>.<metric>.
    val gaugePrefix = s"${ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricAction}.${ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricQualifier}"
    val reported = stdout.linesIterator.filter(_.contains(gaugePrefix)).toSeq

    println(s"\n===== Reporter test ($indexLabel) -- ConsoleMetricsReporter output =====")
    if (reported.isEmpty) println("  (no rli.lookup gauges printed)") else reported.foreach(l => println(s"  ${l.trim}"))
    println("=======================================================================\n")

    assertTrue(reported.nonEmpty,
      s"ConsoleMetricsReporter must publish the '$gaugePrefix' gauges; the drain feeds the reporter and " +
        "commit metadata from a single read, so finding them in the commit but not here means the " +
        "reporter sink regressed")
    Seq(RecordIndexMetricNames.KEY_HIT_COUNT, RecordIndexMetricNames.KEY_MISS_COUNT,
      RecordIndexMetricNames.KEY_COUNT, RecordIndexMetricNames.SHARDS_READ).foreach { metric =>
      val name = s"$gaugePrefix.${RecordIndexMetricNames.key(RecordIndexMetricNames.CALLER_TAG_LOCATION, metric)}"
      assertTrue(stdout.contains(name), s"reporter output must contain the gauge '$name'")
    }
  }

  /** With the feature gated off, neither sink may see anything. */
  @Test
  def testGateOffSuppressesBothSinks(): Unit = {
    val disabledOpts = metricsOpts + (HoodieMetricsConfig.RLI_LOOKUP_METRICS_ENABLE.key -> "false")

    doWriteAndValidateDataAndRecordIndex(disabledOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite,
      validate = false, numInserts = 40)
    clearRliRegistry()

    val stdout = captureStdout {
      doWriteAndValidateDataAndRecordIndex(disabledOpts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append,
        validate = false, numUpdates = 8)
    }

    assertTrue(rliCountersFromLatestCommit().isEmpty, "gate off: nothing may reach commit metadata")
    val gaugePrefix = s"${ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricAction}.${ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricQualifier}"
    assertTrue(!stdout.contains(gaugePrefix), "gate off: nothing may reach the reporter either")
  }
}

/** The same coverage against the partitioned record level index. */
@Tag("functional")
class TestRliLookupMetricsReportingPartitioned extends TestRliLookupMetricsReporting {
  override protected def isPartitionedRli: Boolean = true
}
