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
import org.apache.hudi.common.metrics.Registry
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.metrics.RecordIndexMetricNames

import scala.collection.JavaConverters._

/**
 * Shared plumbing for the record level index lookup metric tests: index selection, and reading the counters back the way an operator would -- off the la
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
   * `commonOpts` turns the global record index on, so the metadata-partition flags and the index type
   * have to be flipped together to select the partitioned variant.
   */
  protected def rliOpts: Map[String, String] = {
    val withTableType = Map(DataSourceWriteOptions.TABLE_TYPE.key -> tableTypeOpt)
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

  protected def counterKey(caller: String, metric: String): String =
    RecordIndexMetricNames.COMMIT_METADATA_PREFIX + RecordIndexMetricNames.key(caller, metric)

  protected def tagKey(metric: String): String =
    counterKey(RecordIndexMetricNames.CALLER_TAG_LOCATION, metric)

  /**
   * A caller that looked something up stamps its full counter set, zeros included, so an absent key means
   * that caller contributed nothing at all. The default is defensive against exactly that case.
   */
  protected def counterOrZero(counters: Map[String, String], caller: String, metric: String): Long =
    counters.getOrElse(counterKey(caller, metric), "0").toLong

  /** The counters as an operator would read them: off the latest completed commit. */
  protected def rliCountersFromLatestCommit(): Map[String, String] = {
    metaClient.reloadActiveTimeline()
    val lastInstant = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()
    metaClient.getActiveTimeline.readCommitMetadata(lastInstant).getExtraMetadata.asScala.toMap
      .filter { case (k, _) => k.startsWith(RecordIndexMetricNames.COMMIT_METADATA_PREFIX) }
  }

  /** Leftover counters from a previous write would otherwise be folded into the next commit. */
  protected def clearRliRegistry(): Unit = {
    Registry.REGISTRY_MAP.asScala.foreach {
      case (key, registry) => if (key.contains(RecordIndexMetricNames.REGISTRY_NAME)) registry.clear()
    }
  }

  /** Asserts the core invariant and returns the looked-up count. */
  protected def assertSumInvariant(counters: Map[String, String], caller: String): Long = {
    val lookedUp = counterOrZero(counters, caller, RecordIndexMetricNames.KEY_COUNT)
    val hits = counterOrZero(counters, caller, RecordIndexMetricNames.KEY_HIT_COUNT)
    val misses = counterOrZero(counters, caller, RecordIndexMetricNames.KEY_MISS_COUNT)
    org.junit.jupiter.api.Assertions.assertEquals(lookedUp, hits + misses,
      s"hits + misses must account for every key looked up by '$caller'")
    // A caller that looked something up must also report the time it took, or the timing metric is
    // silently absent on paths nobody checked. Zero is allowed: a shard read can round below a millisecond.
    if (lookedUp > 0) {
      org.junit.jupiter.api.Assertions.assertTrue(
        counters.contains(counterKey(caller, RecordIndexMetricNames.LOOKUP_TIME)),
        s"'$caller' looked up $lookedUp keys but reported no ${RecordIndexMetricNames.LOOKUP_TIME}; " +
          s"counters were ${counters.keys.toSeq.sorted.mkString(", ")}")
      org.junit.jupiter.api.Assertions.assertTrue(
        counterOrZero(counters, caller, RecordIndexMetricNames.LOOKUP_TIME) >= 0L,
        "elapsed time cannot be negative")
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

  protected def report(label: String, counters: Map[String, String]): Unit = {
    println(s"\n===== $label =====")
    if (counters.isEmpty) {
      println("  (no RLI counters in commit metadata) -- full commit extraMetadata follows:")
      allExtraMetadataFromLatestCommit().toSeq.sorted.foreach { case (k, v) =>
        println(f"    $k%-60s ${v.take(90)}")
      }
    } else {
      counters.toSeq.sorted.foreach { case (k, v) => println(f"  $k%-52s $v") }
    }
    println("=" * (12 + label.length) + "\n")
  }
}
