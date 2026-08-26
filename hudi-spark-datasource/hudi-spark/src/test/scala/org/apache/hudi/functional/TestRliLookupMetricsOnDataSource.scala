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
import org.apache.hudi.metrics.RecordIndexMetricNames
import org.apache.hudi.testutils.CapturingMetricsReporter

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

/** Record level index lookup counters on the Spark DataSource write path. */
@Tag("functional")
class TestRliLookupMetricsOnDataSource extends RliLookupMetricsTestBase {

  /**
   * An upsert of N updates also carries exactly one fresh insert (see
   * `RecordLevelIndexTestBase.doWriteAndValidateDataAndRecordIndex`), so the miss count is 1.
   */
  @Test
  def testCountersReachCommitMetadata(): Unit = {
    val numUpdates = 20

    doWriteAndValidateDataAndRecordIndex(rliOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, validate = false, numInserts = 100)

    doWriteAndValidateDataAndRecordIndex(rliOpts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, validate = false, numUpdates = numUpdates)

    val counters = rliCountersFromLatestCommit()
    report(s"DataSource ($indexLabel) -- RLI counters on the commit", counters)

    assertTrue(counters.nonEmpty, "commit metadata must carry the RLI counters")
    assertEquals(numUpdates.toString, counters(RecordIndexMetricNames.KEY_HIT_COUNT), "every updated key is a hit")
    assertEquals("1", counters(RecordIndexMetricNames.KEY_MISS_COUNT), "the fresh insert is a miss")
    assertEquals(numUpdates + 1L, assertSumInvariant(counters))
    assertTrue(counters(RecordIndexMetricNames.SHARDS_READ).toInt > 0, "at least one shard was read")
  }

  /** Each commit must report only its own work: the drain clears the registry as it publishes. */
  @Test
  def testCountersArePerCommitNotCumulative(): Unit = {
    val numUpdates = 10
    doWriteAndValidateDataAndRecordIndex(rliOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, validate = false, numInserts = 100)

    val perCommit = (1 to 3).map { commit =>
      doWriteAndValidateDataAndRecordIndex(rliOpts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, validate = false, numUpdates = numUpdates)
      val lookedUp = rliCountersFromLatestCommit().getOrElse(RecordIndexMetricNames.KEY_COUNT, "0")
      println(s"[per-commit] commit $commit ($indexLabel): records_looked_up=$lookedUp")
      lookedUp
    }

    perCommit.zipWithIndex.foreach { case (v, i) =>
      assertEquals((numUpdates + 1).toString, v,
        s"commit ${i + 1} must report only its own ${numUpdates + 1} lookups, not a running total")
    }
  }

  /** A commit that performed no lookup must publish nothing of its own. */
  @Test
  def testACommitWithNoLookupCarriesNoCounters(): Unit = {
    doWriteAndValidateDataAndRecordIndex(rliOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite,
      validate = false, numInserts = 60)

    // A commit that does tag, so the registry is drained for the first time.
    doWriteAndValidateDataAndRecordIndex(rliOpts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append,
      validate = false, numUpdates = 10)
    assertTrue(rliCountersFromLatestCommit().nonEmpty, "the upsert must publish counters to drain")

    // The reporter is a stream of emissions, not a running record, so clear what the upsert emitted
    // before the write under test. What remains afterwards is what this commit alone published.
    CapturingMetricsReporter.reset()

    // A plain insert performs no index lookup, so its commit has nothing to report.
    doWriteAndValidateDataAndRecordIndex(rliOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Append,
      validate = false, numInserts = 5)

    val counters = rliCountersFromLatestCommit()
    report(s"DataSource ($indexLabel) -- insert after a drained upsert, expecting nothing", counters)
    assertTrue(counters.isEmpty,
      s"a commit that looked nothing up must publish no counters of its own; got $counters")
  }

  /** The feature gate must suppress the counters entirely, leaving commit metadata untouched. */
  @Test
  def testDisabledConfigEmitsNoCounters(): Unit = {
    val disabledOpts = rliOpts + (HoodieMetricsConfig.RLI_LOOKUP_METRICS_ENABLE.key -> "false")

    doWriteAndValidateDataAndRecordIndex(disabledOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, validate = false, numInserts = 50)
    doWriteAndValidateDataAndRecordIndex(disabledOpts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, validate = false, numUpdates = 10)

    val counters = rliCountersFromLatestCommit()
    report(s"DataSource ($indexLabel) -- gate off, expecting nothing", counters)
    assertTrue(counters.isEmpty,
      s"with ${HoodieMetricsConfig.RLI_LOOKUP_METRICS_ENABLE.key}=false no counters may reach the commit; got $counters")
  }
}

/** The same coverage against the partitioned record level index. */
@Tag("functional")
class TestRliLookupMetricsOnDataSourcePartitioned extends TestRliLookupMetricsOnDataSource {
  override protected def isPartitionedRli: Boolean = true
}

/** The same coverage on Merge On Read. */
@Tag("functional")
class TestRliLookupMetricsOnDataSourceMor extends TestRliLookupMetricsOnDataSource {
  override protected def tableTypeOpt: String = MOR_TABLE_TYPE_OPT_VAL
}

/** MOR against the partitioned record level index. */
@Tag("functional")
class TestRliLookupMetricsOnDataSourceMorPartitioned extends TestRliLookupMetricsOnDataSource {
  override protected def tableTypeOpt: String = MOR_TABLE_TYPE_OPT_VAL
  override protected def isPartitionedRli: Boolean = true
}
