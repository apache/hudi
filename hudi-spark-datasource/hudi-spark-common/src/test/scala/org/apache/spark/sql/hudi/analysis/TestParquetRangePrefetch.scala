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

import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ParquetMetadata}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

import java.util.Collections

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

  @Test
  def testSplitRangesCapsPhysicalReadsAndKeepsChunksAdjacent(): Unit = {
    val split = ParquetRangePrefetch.splitRanges(
      Seq(ParquetRangePrefetch.PlannedRange(10L, 35L, RegionKind.PAGE)),
      maxRangeBytes = 8L)

    assertEquals(
      Seq((10L, 18L), (18L, 26L), (26L, 34L), (34L, 35L)),
      split.map(range => (range.start, range.end)))
  }

  @Test
  def testSplitRangesUsesAvailableConcurrencyBelowConfiguredCap(): Unit = {
    val mebibyte = 1024L * 1024L
    val split = ParquetRangePrefetch.splitRanges(
      Seq(ParquetRangePrefetch.PlannedRange(0L, 40L * mebibyte, RegionKind.PAGE)),
      maxRangeBytes = 8L * mebibyte,
      targetRangeCount = 8)

    assertEquals(8, split.size)
    assertEquals(Seq.fill(8)(5L * mebibyte), split.map(_.length))
  }

  @Test
  def testSplitRangesDoesNotFragmentAnAlreadyParallelPlan(): Unit = {
    val ranges = (0 until 8).map { index =>
      val start = index * 20L
      ParquetRangePrefetch.PlannedRange(start, start + 5L, RegionKind.PAGE)
    }

    val split = ParquetRangePrefetch.splitRanges(
      ranges, maxRangeBytes = 8L, targetRangeCount = 8)

    assertEquals(8, split.size)
    assertEquals(Seq.fill(8)(5L), split.map(_.length))
  }

  @Test
  def testMetadataTailStartsAfterLastColumnChunk(): Unit = {
    val column = mock(classOf[ColumnChunkMetaData])
    when(column.getStartingPos).thenReturn(100L)
    when(column.getTotalSize).thenReturn(50L)
    val block = mock(classOf[BlockMetaData])
    when(block.getColumns).thenReturn(Collections.singletonList(column))
    val metadata = mock(classOf[ParquetMetadata])
    when(metadata.getBlocks).thenReturn(Collections.singletonList(block))

    val tail = ParquetRangePrefetch.metadataTailRange(metadata, fileLength = 200L)

    assertEquals(150L, tail.start)
    assertEquals(200L, tail.end)
    assertEquals(RegionKind.METADATA, tail.kind)
  }
}
