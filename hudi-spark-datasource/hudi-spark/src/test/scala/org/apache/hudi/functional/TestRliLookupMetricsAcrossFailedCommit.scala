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
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy
import org.apache.hudi.common.model.WriteConcurrencyMode
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.{Option => HoodieOption}
import org.apache.hudi.config.{HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieWriteConflictException
import org.apache.hudi.metrics.RecordIndexMetricNames

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

/** A commit that never lands must not take its lookup counters with it. */
@Tag("functional")
class TestRliLookupMetricsAcrossFailedCommit extends RliLookupMetricsTestBase {

  /** Options that make `preCommit` throw, i.e. after the snapshot and before the commit completes. */
  private def conflictingOpts: Map[String, String] = rliOpts ++ Map(
    HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key -> WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name,
    HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key ->
      "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME.key ->
      classOf[AlwaysConflictingResolutionStrategy].getName)

  private def causeChain(t: Throwable): String = {
    var current = t
    val sb = new StringBuilder
    while (current != null) {
      sb.append(current.toString).append(" | ")
      current = current.getCause
    }
    sb.toString
  }

  @Test
  def testCountersSurviveACommitThatNeverLands(): Unit = {
    val failedUpdates = 10
    val retriedUpdates = 4
    // Each upsert batch carries one fresh insert alongside its updates, so it looks up N + 1 keys.
    val failedLookups = failedUpdates + 1
    val retriedLookups = retriedUpdates + 1

    doWriteAndValidateDataAndRecordIndex(rliOpts, INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite,
      validate = false, numInserts = 80)
    clearRliRegistry()
    assertTrue(rliCountersFromLatestCommit().isEmpty, "the seeding insert performs no lookup")

    // An upsert whose commit is rejected in preCommit. The lookups happened; the commit did not.
    val failure = try {
      doWriteAndValidateDataAndRecordIndex(conflictingOpts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append,
        validate = false, numUpdates = failedUpdates)
      None
    } catch {
      case t: Throwable => Some(t)
    }
    assertTrue(failure.isDefined, "the injected conflict must fail the write")
    assertTrue(causeChain(failure.get).contains(AlwaysConflictingResolutionStrategy.MESSAGE),
      s"the write must have failed on the injected conflict, not on something else: ${causeChain(failure.get)}")

    // Nothing was published for it: the newest completed commit is still the seeding insert.
    val afterFailure = rliCountersFromLatestCommit()
    report(s"Failed commit ($indexLabel) -- latest completed commit, expected empty", afterFailure)
    assertTrue(afterFailure.isEmpty,
      s"a commit that never landed must not leave counters on the timeline; got $afterFailure")

    // The next commit to succeed reports its own lookups plus the ones the failed attempt performed.
    doWriteAndValidateDataAndRecordIndex(rliOpts, UPSERT_OPERATION_OPT_VAL, SaveMode.Append,
      validate = false, numUpdates = retriedUpdates)

    val counters = rliCountersFromLatestCommit()
    report(s"Retry after failed commit ($indexLabel) -- expected ${failedLookups + retriedLookups}", counters)

    assertTrue(counters.nonEmpty, "the retry must carry counters")
    assertEquals((failedLookups + retriedLookups).toLong,
      assertSumInvariant(counters, RecordIndexMetricNames.CALLER_TAG_LOCATION),
      "the retry must report the failed attempt's lookups as well as its own; releasing the counters " +
        "before the commit completed would have dropped the failed attempt's " + failedLookups)
  }
}

object AlwaysConflictingResolutionStrategy {
  val MESSAGE = "injected conflict: this commit must not land"
}

/**
 * Fails conflict resolution unconditionally, which is the first thing `preCommit` does. Loaded reflectively
 * from `hoodie.write.lock.conflict.resolution.strategy`, so it needs a no-argument constructor.
 */
class AlwaysConflictingResolutionStrategy extends SimpleConcurrentFileWritesConflictResolutionStrategy {
  override def getCandidateInstants(metaClient: HoodieTableMetaClient,
                                    currentInstant: HoodieInstant,
                                    lastSuccessfulInstant: HoodieOption[HoodieInstant]): java.util.stream.Stream[HoodieInstant] =
    throw new HoodieWriteConflictException(AlwaysConflictingResolutionStrategy.MESSAGE)
}

/** The same coverage against the partitioned record level index. */
@Tag("functional")
class TestRliLookupMetricsAcrossFailedCommitPartitioned extends TestRliLookupMetricsAcrossFailedCommit {
  override protected def isPartitionedRli: Boolean = true
}
