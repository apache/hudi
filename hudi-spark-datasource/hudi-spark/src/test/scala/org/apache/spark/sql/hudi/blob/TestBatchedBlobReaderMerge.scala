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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.blob

import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Direct unit tests for the merge algorithm in {@link BatchedBlobReader}.
 *
 * These tests bypass Spark and storage entirely: they construct a reader with
 * a null storage (the merge methods do not touch I/O) and call mergeRanges /
 * identifyConsecutiveRanges with crafted RowInfo inputs to assert the
 * structure of the merged output (counts, boundaries, ordering, grouping).
 *
 * This complements TestBatchedBlobReader (which verifies data correctness end-to-end)
 * by proving the batching algorithm produces the expected merged ranges for
 * each input pattern.
 */
class TestBatchedBlobReaderMerge {

  private def reader(maxGapBytes: Int = 4096) =
    new BatchedBlobReader(storage = null, maxGapBytes = maxGapBytes, lookaheadRows = 50)

  private def row(filePath: String, offset: Long, length: Long, index: Long = 0L): RowInfo[Row] =
    RowInfo[Row](
      originalRow = null.asInstanceOf[Row],
      filePath = filePath,
      offset = offset,
      length = length,
      index = index)

  @Test
  def testEmptyInputProducesNoRanges(): Unit = {
    val merged = reader().mergeRanges(Seq.empty[RowInfo[Row]], maxGap = 100)
    assertTrue(merged.isEmpty)
  }

  @Test
  def testSingleRowProducesSingleRange(): Unit = {
    val merged = reader().mergeRanges(Seq(row("/f", 1000, 200, index = 7)), maxGap = 100)
    assertEquals(1, merged.size)
    val r = merged.head
    assertEquals("/f", r.filePath)
    assertEquals(1000L, r.startOffset)
    assertEquals(1200L, r.endOffset)
    assertEquals(1, r.rows.size)
    assertEquals(7L, r.rows.head.index)
  }

  @Test
  def testContiguousRangesMergeIntoOne(): Unit = {
    // Zero-gap reads: [0,100), [100,100), [200,100) — gap == 0 <= maxGap == 0 → merge
    val rows = Seq(
      row("/f", 0, 100, index = 0),
      row("/f", 100, 100, index = 1),
      row("/f", 200, 100, index = 2))
    val merged = reader().mergeRanges(rows, maxGap = 0)
    assertEquals(1, merged.size)
    assertEquals(0L, merged.head.startOffset)
    assertEquals(300L, merged.head.endOffset)
    assertEquals(3, merged.head.rows.size)
  }

  @Test
  def testGapEqualToThresholdMerges(): Unit = {
    // Gaps of exactly maxGap → inclusive boundary, merge into one
    val rows = Seq(
      row("/f", 0, 100),
      row("/f", 1124, 100), // gap = 1024 == maxGap
      row("/f", 2248, 100)) // gap = 1024 == maxGap
    val merged = reader().mergeRanges(rows, maxGap = 1024)
    assertEquals(1, merged.size)
    assertEquals(0L, merged.head.startOffset)
    assertEquals(2348L, merged.head.endOffset)
  }

  @Test
  def testGapOneOverThresholdSplits(): Unit = {
    // Gap of maxGap + 1 → split
    val rows = Seq(
      row("/f", 0, 100),
      row("/f", 1125, 100)) // gap = 1025 > maxGap = 1024
    val merged = reader().mergeRanges(rows, maxGap = 1024)
    assertEquals(2, merged.size)
    assertEquals(0L, merged.head.startOffset)
    assertEquals(100L, merged.head.endOffset)
    assertEquals(1125L, merged(1).startOffset)
    assertEquals(1225L, merged(1).endOffset)
  }

  @Test
  def testLargeGapsProduceSeparateRanges(): Unit = {
    val rows = Seq(
      row("/f", 0, 100, index = 0),
      row("/f", 10000, 100, index = 1),
      row("/f", 20000, 100, index = 2),
      row("/f", 30000, 100, index = 3))
    val merged = reader().mergeRanges(rows, maxGap = 1000)
    assertEquals(4, merged.size)
    merged.foreach(r => assertEquals(1, r.rows.size))
  }

  @Test
  def testUnsortedInputIsSortedBeforeMerge(): Unit = {
    // identifyConsecutiveRanges sorts by offset within each file
    val rows = Seq(
      row("/f", 200, 100, index = 2),
      row("/f", 0, 100, index = 0),
      row("/f", 100, 100, index = 1))
    val merged = reader(maxGapBytes = 0).identifyConsecutiveRanges(rows)
    assertEquals(1, merged.size, "Single merged range expected after sort + merge")
    assertEquals(0L, merged.head.startOffset)
    assertEquals(300L, merged.head.endOffset)
    // Internal rows in the merged range follow sorted order
    val offsetsInMerged = merged.head.rows.map(_.offset)
    assertEquals(Seq(0L, 100L, 200L), offsetsInMerged)
  }

  @Test
  def testMultipleFilesProduceRangePerFile(): Unit = {
    val rows = Seq(
      row("/a", 0, 100, index = 0),
      row("/b", 0, 100, index = 1),
      row("/a", 100, 100, index = 2),
      row("/b", 100, 100, index = 3))
    val merged = reader(maxGapBytes = 4096).identifyConsecutiveRanges(rows)
    assertEquals(2, merged.size, "One merged range per file")
    val byFile = merged.groupBy(_.filePath)
    assertEquals(Set("/a", "/b"), byFile.keySet)
    assertEquals(1, byFile("/a").size)
    assertEquals(1, byFile("/b").size)
    // Each file's merged range covers offsets 0..200
    byFile.values.foreach { rs =>
      assertEquals(0L, rs.head.startOffset)
      assertEquals(200L, rs.head.endOffset)
      assertEquals(2, rs.head.rows.size)
    }
  }

  @Test
  def testRowIndicesPreservedInMergedRange(): Unit = {
    // Indices retained so the downstream sort-by-index can reconstruct input order
    val rows = Seq(
      row("/f", 0, 100, index = 42),
      row("/f", 100, 100, index = 7),
      row("/f", 200, 100, index = 19))
    val merged = reader().mergeRanges(rows, maxGap = 4096)
    assertEquals(1, merged.size)
    val indices = merged.head.rows.map(_.index)
    assertEquals(Seq(42L, 7L, 19L), indices)
  }

  @Test
  def testOverlappingRangesThrow(): Unit = {
    // [0,100) followed by [50,100) -> overlap
    val rows = Seq(
      row("/f", 0, 100, index = 0),
      row("/f", 50, 100, index = 1))
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => reader().mergeRanges(rows, maxGap = 4096))
    assertTrue(ex.getMessage.contains("Overlapping blob ranges detected"))
  }
}
