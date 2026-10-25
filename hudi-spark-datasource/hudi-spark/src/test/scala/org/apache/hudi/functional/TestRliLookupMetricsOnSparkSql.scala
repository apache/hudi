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
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.metrics.RecordIndexLookupMetrics
import org.apache.hudi.testutils.CapturingMetricsReporter

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

/** Record level index lookup counters on the Spark SQL write path. */
@Tag("functional")
class TestRliLookupMetricsOnSparkSql extends RliLookupMetricsTestBase {

  private val sqlTable = "rli_lookup_metrics_tbl"
  private val numSeedRecords = 60

  /**
   * Seeds a table through the DataSource so the record index exists, then exposes it to SQL and applies
   * the index settings as session configs -- index type is a write config, not a table property.
   */
  private def seedTableAndRegisterForSql(): Unit = {
    doWriteAndValidateDataAndRecordIndex(rliOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite,
      validate = false, numInserts = numSeedRecords)

    spark.sql(s"drop table if exists $sqlTable")
    spark.sql(s"create table $sqlTable using hudi location '$basePath'")

    spark.sql("set hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.InProcessLockProvider")
    spark.sql(s"set ${HoodieMetadataConfig.ENABLE.key} = true")
    spark.sql(s"set ${HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key} = ${!isPartitionedRli}")
    spark.sql(s"set ${HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key} = $isPartitionedRli")
    spark.sql(s"set ${HoodieIndexConfig.INDEX_TYPE.key} = " +
      (if (isPartitionedRli) "RECORD_LEVEL_INDEX" else "GLOBAL_RECORD_LEVEL_INDEX"))

    // SQL DML does not inherit the DataSource options the seed write used, so the reporter has to be
    // selected as a session config too or the write publishes nowhere the test can read.
    metricsOpts.foreach { case (key, value) => spark.sql(s"set $key = $value") }
    CapturingMetricsReporter.reset()
  }

  /**
   * The default path. Optimized writes make UPDATE a prepped write, so no index lookup happens and no
   * counters are produced. Documented behaviour, asserted so it cannot change unnoticed.
   */
  @Test
  def testUpdateWithOptimizedWritesPerformsNoLookup(): Unit = {
    seedTableAndRegisterForSql()
    spark.sql(s"set ${SPARK_SQL_OPTIMIZED_WRITES.key} = true")

    spark.sql(s"update $sqlTable set rider = 'rider-optimized'")

    val counters = rliCountersFromLatestCommit()
    report(s"Spark SQL UPDATE, optimized writes ON ($indexLabel) -- expected empty", counters)
    assertTrue(counters.isEmpty,
      "a prepped UPDATE already knows each record's location, so it performs no RLI lookup to report")
  }

  /**
   * With optimized writes disabled the UPDATE goes through normal tagging, and every touched key is
   * looked up in the index. No predicate means every row, so the counts are exact.
   */
  @Test
  def testUpdateWithoutOptimizedWritesPublishesCounters(): Unit = {
    seedTableAndRegisterForSql()
    spark.sql(s"set ${SPARK_SQL_OPTIMIZED_WRITES.key} = false")

    spark.sql(s"update $sqlTable set rider = 'rider-updated'")

    val counters = rliCountersFromLatestCommit()
    report(s"Spark SQL UPDATE, optimized writes OFF ($indexLabel)", counters)

    assertTrue(counters.nonEmpty, "a non-prepped UPDATE must publish RLI counters at its commit")
    val lookedUp = assertSumInvariant(counters)
    assertEquals(numSeedRecords.toLong, lookedUp, "UPDATE with no predicate tags every row")
    assertEquals(numSeedRecords.toString, counters(RecordIndexLookupMetrics.KEY_HIT_COUNT),
      "every row being updated already exists in the index")
    // A caller that looked something up stamps its whole counter set, zeros included, so the record on the
    // timeline is internally consistent. Dropping just the zero components would leave a commit reporting
    // hits and records_looked_up but no misses, forcing every consumer to treat absent as zero.
    assertTrue(counters.contains(RecordIndexLookupMetrics.KEY_MISS_COUNT),
      "a caller that looked keys up reports its full counter set, including the zeros")
    assertEquals("0", counters(RecordIndexLookupMetrics.KEY_MISS_COUNT), "no new keys are introduced")
    assertTrue(counters(RecordIndexLookupMetrics.SHARDS_READ).toInt > 0, "at least one shard was read")
  }

  /** MERGE INTO is not a prepped write, so its keys are tagged and must be counted. */
  @Test
  def testMergeIntoPublishesCounters(): Unit = {
    seedTableAndRegisterForSql()

    val numMerged = 25
    spark.sql(
      s"""create or replace temporary view rli_merge_src as
         |select _row_key, partition, timestamp, 'rider-merged' as rider
         |from $sqlTable limit $numMerged""".stripMargin)

    spark.sql(
      s"""merge into $sqlTable t
         |using rli_merge_src s
         |on t._row_key = s._row_key
         |when matched then update set t.rider = s.rider, t.timestamp = s.timestamp""".stripMargin)

    val counters = rliCountersFromLatestCommit()
    report(s"Spark SQL MERGE INTO ($indexLabel)", counters)

    assertTrue(counters.nonEmpty, "a MERGE INTO must publish RLI counters at its commit")
    val lookedUp = assertSumInvariant(counters)
    assertEquals(numMerged.toLong, lookedUp, s"the merge tags its $numMerged matched keys")
    assertEquals(numMerged.toString, counters(RecordIndexLookupMetrics.KEY_HIT_COUNT),
      "every merged key already exists in the index")
  }
}

/** The same coverage against the partitioned record level index. */
@Tag("functional")
class TestRliLookupMetricsOnSparkSqlPartitioned extends TestRliLookupMetricsOnSparkSql {
  override protected def isPartitionedRli: Boolean = true
}
