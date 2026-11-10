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
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.metrics.RecordIndexLookupMetrics
import org.apache.hudi.testutils.CapturingMetricsReporter

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.JavaConverters._

/** Record level index lookup counters on the Spark DataSource write path. */
@Tag("functional")
class TestRliLookupMetricsOnDataSource extends RliLookupMetricsTestBase {

  /**
   * An upsert of N updates also carries exactly one fresh insert (see
   * `RecordLevelIndexTestBase.doWriteAndValidateDataAndRecordIndex`), so the miss count is 1.
   */
  @Test
  def testCountersReachTheReporter(): Unit = {
    val numUpdates = 20

    doWriteAndValidateDataAndRecordIndex(rliOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, validate = false, numInserts = 100)

    doWriteAndValidateDataAndRecordIndex(rliOpts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, validate = false, numUpdates = numUpdates)

    val counters = rliCountersFromLatestCommit()
    report(s"DataSource ($indexLabel) -- RLI counters on the commit", counters)

    assertTrue(counters.nonEmpty, "the reporter must carry the RLI counters")
    assertEquals(numUpdates.toString, counters(RecordIndexLookupMetrics.KEY_HIT_COUNT), "every updated key is a hit")
    assertEquals("1", counters(RecordIndexLookupMetrics.KEY_MISS_COUNT), "the fresh insert is a miss")
    assertEquals(numUpdates + 1L, assertSumInvariant(counters))
    assertTrue(counters(RecordIndexLookupMetrics.SHARDS_READ).toInt > 0, "at least one shard was read")
  }

  /** Each commit must report only its own work: the drain clears the registry as it publishes. */
  @Test
  def testCountersArePerCommitNotCumulative(): Unit = {
    val numUpdates = 10
    doWriteAndValidateDataAndRecordIndex(rliOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, validate = false, numInserts = 100)

    val perCommit = (1 to 3).map { commit =>
      doWriteAndValidateDataAndRecordIndex(rliOpts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append, validate = false, numUpdates = numUpdates)
      val lookedUp = rliCountersFromLatestCommit().getOrElse(RecordIndexLookupMetrics.KEY_COUNT, "0")
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

  /**
   * INSERT with drop-dups resolves duplicates through a record index lookup before the write, and that
   * lookup runs on the committing client's engine context. Its counters must therefore reach the reporter
   * at the INSERT's commit. Regression guard: dedup used to run on a throwaway context (built inside
   * `DataSourceUtils`) whose registry `postCommit` never drained, so the INSERT published nothing.
   */
  @Test
  def testInsertDropDupsPublishesDedupLookupCounters(): Unit = {
    val numSeed = 100
    val numReoffered = 40 // existing keys offered again -> dropped as duplicates -> hits
    val numFresh = 15 // brand-new keys -> kept -> misses

    // Seed the table so the record index has keys to hit.
    val seedBatch = doWriteAndValidateDataAndRecordIndex(rliOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite,
      validate = false, numInserts = numSeed)

    // A batch mixing already-present keys with new ones, so both hits and misses are exercised.
    val freshBatch = recordsToStrings(dataGen.generateInsertsAsPerSchema(
      getInstantTime(), numFresh, HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)).asScala
    val freshDf = spark.read.json(spark.sparkContext.parallelize(freshBatch.toSeq, 2))
    val insertBatch = seedBatch.limit(numReoffered).unionByName(freshDf)

    // Isolate the INSERT under test from the seed's emissions.
    CapturingMetricsReporter.reset()

    insertBatch.write.format("hudi")
      .options(rliOpts)
      .option(OPERATION.key, INSERT_OPERATION_OPT_VAL)
      .option(INSERT_DROP_DUPS.key, "true")
      .mode(SaveMode.Append)
      .save(basePath)

    val counters = rliCountersFromLatestCommit()
    report(s"DataSource INSERT drop-dups ($indexLabel)", counters)

    assertTrue(counters.nonEmpty,
      "INSERT with drop-dups resolves duplicates via an RLI lookup; its counters must reach the reporter")
    assertEquals(numReoffered.toString, counters(RecordIndexLookupMetrics.KEY_HIT_COUNT),
      "the re-offered keys already exist in the index")
    assertEquals(numFresh.toString, counters(RecordIndexLookupMetrics.KEY_MISS_COUNT),
      "the brand-new keys are misses")
    assertEquals((numReoffered + numFresh).toLong, assertSumInvariant(counters),
      "the dedup lookup examines every incoming record")
    assertTrue(counters(RecordIndexLookupMetrics.SHARDS_READ).toInt > 0, "at least one shard was read")
  }

}

/** The same coverage against the partitioned record level index. */
@Tag("functional")
class TestRliLookupMetricsOnDataSourcePartitioned extends TestRliLookupMetricsOnDataSource {
  override protected def isPartitionedRli: Boolean = true
}
