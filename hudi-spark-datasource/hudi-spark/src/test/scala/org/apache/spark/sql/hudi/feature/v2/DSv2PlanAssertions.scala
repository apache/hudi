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

package org.apache.spark.sql.hudi.feature.v2

import org.apache.spark.sql.DataFrame
import org.junit.jupiter.api.Assertions.assertTrue

/**
 * Plan-shape assertion helpers shared across DSv2 functional tests. Mix in to pick up the
 * standard BatchScan/FileScan checks instead of redefining them per test class.
 */
trait DSv2PlanAssertions {

  protected def containsBatchScan(plan: String): Boolean = plan.contains("BatchScan")

  protected def containsFileScan(plan: String): Boolean = plan.contains("FileScan")

  // A fully pushed-down aggregate (e.g. COUNT(*) from column stats) materializes as
  // Spark's LocalTableScan (via HoodieLocalScan/LocalScan), so accept either form as
  // a DSv2 indicator in tests that may exercise aggregate pushdown. Not a strict
  // BatchScan guard — tests that need that should call containsBatchScan directly.
  protected def usesDsv2ReadPath(plan: String): Boolean =
    plan.contains("BatchScan") || plan.contains("LocalTableScan")

  /** Assert `df`'s executed plan reads through the DSv2 BatchScan node. */
  protected def assertBatchScan(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.toString()
    assertTrue(containsBatchScan(plan),
      s"DSv2 read should use BatchScan, but plan was:\n$plan")
  }

  /** Assert `df`'s executed plan reads through the DSv1 FileScan node. */
  protected def assertFileScan(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.toString()
    assertTrue(containsFileScan(plan),
      s"DSv1 read should use FileScan, but plan was:\n$plan")
  }

  /**
   * Assert `df`'s executed plan reads through DSv2 — either BatchScan or
   * LocalTableScan (from a fully pushed-down aggregate) — and not DSv1 FileScan.
   */
  protected def assertDsv2ReadPath(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.toString()
    assertTrue(usesDsv2ReadPath(plan) && !containsFileScan(plan),
      s"DSv2 read should use BatchScan or LocalTableScan, but plan was:\n$plan")
  }
}
