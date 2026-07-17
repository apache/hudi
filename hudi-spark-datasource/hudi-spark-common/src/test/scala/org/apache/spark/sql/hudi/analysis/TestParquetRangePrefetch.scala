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

import org.apache.hudi.io.storage.HoodiePrefetchedParquetInputFile.RegionKind

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestParquetRangePrefetch {

  @Test
  def testMergeRangesUsesUnionOfPlannerOutput(): Unit = {
    val plannerOutput = Seq(
      ParquetRangePrefetch.PlannedRange(30, 40, RegionKind.PAGE),
      ParquetRangePrefetch.PlannedRange(10, 20, RegionKind.DICTIONARY),
      ParquetRangePrefetch.PlannedRange(15, 35, RegionKind.PAGE),
      ParquetRangePrefetch.PlannedRange(1024 * 1024, 1024 * 1024 + 10, RegionKind.PAGE))

    // Independent test-side coalescing oracle: nearby overlaps form [10, 40), while
    // the range over 512 KiB away remains separate.
    val expected = Seq((10L, 40L), (1024L * 1024L, 1024L * 1024L + 10L))
    val actual = ParquetRangePrefetch.mergeRanges(plannerOutput).map(range => (range.start, range.end))

    assertEquals(expected, actual)
  }
}
