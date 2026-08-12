/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.common.index.vector.{VectorIndexOptions, VectorStalePolicy}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.timeline.versioning.DefaultInstantGenerator
import org.apache.hudi.common.testutils.MockHoodieTimeline

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters._

class TestIvfRaBitQMdtSearchAlgorithm {

  @Test
  def testApproximateCandidateHeapUsesSmallSafetyFloor(): Unit = {
    assertEquals(32, IvfRaBitQMdtSearchAlgorithm.approximateCandidateHeapSize(10))
  }

  @Test
  def testApproximateCandidateHeapDoesNotCapLargeTopK(): Unit = {
    assertEquals(50, IvfRaBitQMdtSearchAlgorithm.approximateCandidateHeapSize(50))
  }

  @Test
  def testRuntimeModeOverrideRecomputesImplicitFreshnessButPreservesExplicitOverride(): Unit = {
    val indexOptions = Map(
      VectorIndexOptions.QUERY_MODE -> "exact_rerank",
      VectorIndexOptions.FRESHNESS_POLICY -> "fail")
    val approximateOptions = IvfRaBitQMdtSearchAlgorithm.resolvedTuningOptions(
      indexOptions,
      Map(VectorIndexOptions.QUERY_MODE -> "approximate"))

    assertEquals(
      VectorStalePolicy.WARN,
      VectorIndexOptions.getFreshnessPolicy(approximateOptions.asJava))

    val explicitlyOverridden = IvfRaBitQMdtSearchAlgorithm.resolvedTuningOptions(
      indexOptions,
      Map(
        VectorIndexOptions.QUERY_MODE -> "approximate",
        VectorIndexOptions.FRESHNESS_POLICY -> "fallback"))
    assertEquals(
      VectorStalePolicy.FALLBACK,
      VectorIndexOptions.getFreshnessPolicy(explicitlyOverridden.asJava))
  }

  @Test
  def testCompletedSourceWriteTimelineIgnoresCleanAndOtherNonWriteActions(): Unit = {
    val generator = new DefaultInstantGenerator
    val timeline = new MockHoodieTimeline(Seq(
      generator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001"),
      generator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "002"),
      generator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.INDEXING_ACTION, "003"),
      generator.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "004")
    ).asJava)

    val sourceWrites = IvfRaBitQMdtSearchAlgorithm.completedSourceWriteTimeline(timeline)
      .getInstantsAsStream.iterator.asScala.toSeq

    assertEquals(Seq("001"), sourceWrites.map(_.requestedTime()))
    assertFalse(sourceWrites.exists(_.getAction == HoodieTimeline.CLEAN_ACTION))
  }

  @Test
  def testFreshnessLagReportsFirstUnmarkedInstantAndCount(): Unit = {
    val lag = IvfRaBitQMdtSearchAlgorithm
      .freshnessLag(Seq("001", "002", "003"), Some("001"))
      .get

    assertEquals("002", lag.firstUnmarkedInstant)
    assertEquals(2, lag.lagCount)
    assertEquals(None, IvfRaBitQMdtSearchAlgorithm.freshnessLag(Seq("001"), Some("001")))
  }
}
