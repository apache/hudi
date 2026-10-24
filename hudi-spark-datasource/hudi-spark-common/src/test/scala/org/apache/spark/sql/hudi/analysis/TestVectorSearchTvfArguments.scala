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

package org.apache.spark.sql.hudi.analysis

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{HoodieVectorSearchBatchTableValuedFunction, HoodieVectorSearchTableValuedFunction}
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class TestVectorSearchTvfArguments {
  private val single: Seq[Expression] = Seq(
    Literal("corpus"), Literal("embedding"), Literal(1.0), Literal(10), Literal("l2"))
  private val batch: Seq[Expression] = Seq(
    Literal("corpus"), Literal("embedding"), Literal("queries"), Literal("embedding"),
    Literal(10), Literal("l2"))

  @Test
  def testSingleBruteForceLegacyFilterAndDistance(): Unit = {
    val args = HoodieVectorSearchTableValuedFunction.parseArgs(
      single ++ Seq(Literal("brute_force"), Literal("id > 3"), Literal(0.5)))
    assertEquals("id > 3", args.runtimeOptions(
      HoodieVectorSearchTableValuedFunction.BRUTE_FORCE_FILTER_OPT))
    assertEquals("0.5", args.runtimeOptions(
      HoodieVectorSearchTableValuedFunction.BRUTE_FORCE_MAX_DISTANCE_OPT))
    assertTrue(HoodieVectorSearchTableValuedFunction.parseArgs(
      single ++ Seq(Literal("brute_force"), Literal(null), Literal(null))).runtimeOptions.isEmpty)
  }

  @Test
  def testSingleIvfRuntimeOptionsAreNotMisreadAsFilter(): Unit = {
    val args = HoodieVectorSearchTableValuedFunction.parseArgs(
      single ++ Seq(Literal("ivf_rabitq_mdt"), Literal("vector.query.nprobes=16")))
    assertEquals(Map("vector.query.nprobes" -> "16"), args.runtimeOptions)
    assertThrows(classOf[HoodieAnalysisException], () =>
      HoodieVectorSearchTableValuedFunction.parseArgs(
        single ++ Seq(Literal("ivf_rabitq_mdt"), Literal("vector.query.nprobes=16"), Literal(0.5))))
  }

  @Test
  def testBatchBruteForceLegacyFilterAndDistance(): Unit = {
    val args = HoodieVectorSearchBatchTableValuedFunction.parseArgs(
      batch ++ Seq(Literal("brute_force"), Literal("id > 3"), Literal(1)))
    assertEquals("id > 3", args.runtimeOptions(
      HoodieVectorSearchTableValuedFunction.BRUTE_FORCE_FILTER_OPT))
    assertEquals("1.0", args.runtimeOptions(
      HoodieVectorSearchTableValuedFunction.BRUTE_FORCE_MAX_DISTANCE_OPT))
  }

  @Test
  def testBatchIvfRuntimeOptions(): Unit = {
    val args = HoodieVectorSearchBatchTableValuedFunction.parseArgs(
      batch ++ Seq(Literal("ivf_rabitq_mdt"), Literal("vector.query.refine_factor=20")))
    assertEquals(Map("vector.query.refine_factor" -> "20"), args.runtimeOptions)
  }

  @Test
  def testBruteForceRejectsStringDistance(): Unit = {
    assertThrows(classOf[HoodieAnalysisException], () =>
      HoodieVectorSearchTableValuedFunction.parseArgs(
        single ++ Seq(Literal("brute_force"), Literal("id > 3"), Literal("0.5"))))
  }
}
