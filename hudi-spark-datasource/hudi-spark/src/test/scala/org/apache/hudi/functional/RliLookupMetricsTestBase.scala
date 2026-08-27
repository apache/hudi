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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.config.metrics.HoodieMetricsConfig
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.metrics.{ExecutorMetricRegistry, MetricsReporterType, RecordIndexMetricNames}
import org.apache.hudi.testutils.CapturingMetricsReporter

import scala.collection.JavaConverters._

/**
 * Shared plumbing for the record level index lookup metric tests: index selection, and reading the
 * counters back the way an operator would -- off the configured metrics reporter.
 */
abstract class RliLookupMetricsTestBase extends RecordLevelIndexTestBase {

  /** Overridden by the partitioned subclasses; both variants are separate closures on separate paths. */
  protected def isPartitionedRli: Boolean = false

  /**
   * Table type under test. Tagging is an index-level concern and does not branch on table type, so MOR
   * is expected to behave identically -- the MOR subclasses exist to prove that rather than assume it.
   */
  protected def tableTypeOpt: String = DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL

  protected def indexLabel: String = {
    val idx = if (isPartitionedRli) "partitioned RLI" else "global RLI"
    val tt = if (tableTypeOpt == DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL) "MOR" else "COW"
    s"$idx, $tt"
  }

  /**
   * The drain reports to the configured reporter, so these tests need one. `commonOpts` turns the global
   * record index on, so the metadata-partition flags and the index type are flipped together below to
   * select the partitioned variant.
   */
  protected def metricsOpts: Map[String, String] = Map(
    HoodieMetricsConfig.TURN_METRICS_ON.key -> "true",
    // Explicit: collection is opted into by name, not inherited from metrics being on.
    HoodieMetricsConfig.RLI_LOOKUP_METRICS_ENABLE.key -> "true",
    // The type still has to be set: it defaults to GRAPHITE, whose config builder NPEs without a prefix.
    // The factory prefers the class when one is given, so this only keeps the builder happy.
    HoodieMetricsConfig.METRICS_REPORTER_TYPE_VALUE.key -> MetricsReporterType.INMEMORY.name(),
    HoodieMetricsConfig.METRICS_REPORTER_CLASS_NAME.key -> classOf[CapturingMetricsReporter].getName)

  protected def rliOpts: Map[String, String] = {
    val withTableType = Map(DataSourceWriteOptions.TABLE_TYPE.key -> tableTypeOpt) ++ metricsOpts
    if (isPartitionedRli) {
      commonOpts ++ withTableType ++ Map(
        HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false",
        HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
        HoodieIndexConfig.INDEX_TYPE.key -> "RECORD_LEVEL_INDEX")
    } else {
      commonOpts ++ withTableType ++ Map(
        HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
        HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false",
        HoodieIndexConfig.INDEX_TYPE.key -> "GLOBAL_RECORD_LEVEL_INDEX")
    }
  }

  /**
   * A lookup that happened stamps the full counter set, zeros included, so an absent key means nothing was
   * looked up at all. The default is defensive against exactly that case.
   */
  protected def counterOrZero(counters: Map[String, String], metric: String): Long =
    counters.getOrElse(metric, "0").toLong

  /**
   * The counters as an operator would read them: off the metrics reporter. {@code Metrics} is keyed by
   * base path, so building a handle here returns the same instance the write published into.
   */
  protected def rliCountersFromLatestCommit(): Map[String, String] = {
    val marker = "." + ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricAction() +
      "." + ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.metricQualifier() + "."
    CapturingMetricsReporter.captured().asScala.toMap
      .collect { case (name, value) if name.contains(marker) =>
        name.substring(name.indexOf(marker) + marker.length) -> value.toString }
  }

  /** The reporter is process-wide, so each test starts from a clean slate. */
  @org.junit.jupiter.api.BeforeEach
  def resetCapturedMetrics(): Unit = CapturingMetricsReporter.reset()


  /** Asserts the core invariant and returns the looked-up count. */
  protected def assertSumInvariant(counters: Map[String, String]): Long = {
    val lookedUp = counterOrZero(counters, RecordIndexMetricNames.KEY_COUNT)
    val hits = counterOrZero(counters, RecordIndexMetricNames.KEY_HIT_COUNT)
    val misses = counterOrZero(counters, RecordIndexMetricNames.KEY_MISS_COUNT)
    org.junit.jupiter.api.Assertions.assertEquals(lookedUp, hits + misses,
      "hits + misses must account for every key looked up")
    // A lookup that happened must also report the time it took, or the timing metric is silently absent on
    // paths nobody checked. Zero is allowed: a shard read can round below a millisecond.
    if (lookedUp > 0) {
      org.junit.jupiter.api.Assertions.assertTrue(
        counters.contains(RecordIndexMetricNames.LOOKUP_TIME),
        s"looked up $lookedUp keys but reported no ${RecordIndexMetricNames.LOOKUP_TIME}; " +
          s"counters were ${counters.keys.toSeq.sorted.mkString(", ")}")
    }
    lookedUp
  }

  /** Everything on the latest commit, for diagnosing an unexpectedly empty counter set. */
  protected def allExtraMetadataFromLatestCommit(): Map[String, String] = {
    metaClient.reloadActiveTimeline()
    val lastInstant = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()
    metaClient.getActiveTimeline.readCommitMetadata(lastInstant).getExtraMetadata.asScala.toMap +
      ("__instant" -> lastInstant.toString)
  }

  /**
   * Puts counters into the table's registry the way an attempt that never committed would leave them.
   * Cheaper and more direct than injecting a conflict, and it isolates the behaviour under test: what a
   * later commit does with counters it did not produce.
   */
  protected def seedCountersAsAbandonedAttempt(keyCount: Long): Unit = {
    metaClient.reloadTableConfig()
    val key = org.apache.hudi.common.metrics.Registry.makeKey(
      metaClient.getTableConfig.getTableName,
      ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.scopedName(basePath))
    val registry = org.apache.hudi.common.metrics.Registry.REGISTRY_MAP.get(key)
    assert(registry != null, s"expected a registry at $key; the seeding write should have created one")
    registry.add(RecordIndexMetricNames.KEY_COUNT, keyCount)
    registry.add(RecordIndexMetricNames.KEY_HIT_COUNT, keyCount)
  }

  protected def report(label: String, counters: Map[String, String]): Unit = {
    println(s"\n===== $label =====")
    if (counters.isEmpty) {
      println("  (no RLI counters published) -- every gauge the reporter holds follows:")
      CapturingMetricsReporter.captured().asScala.toSeq.sortBy(_._1).foreach { case (k, v) =>
        println(f"    $k%-70s $v")
      }
    } else {
      counters.toSeq.sorted.foreach { case (k, v) => println(f"  $k%-52s $v") }
    }
    println("=" * (12 + label.length) + "\n")
  }
}
