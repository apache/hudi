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

import org.apache.hudi.blob.BlobTestHelpers._
import org.apache.hudi.storage.{HoodieStorage, HoodieStorageUtils}
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Integration tests asserting that batched blob reads actually reduce storage I/O.
 *
 * Each scenario drives BatchedBlobReader.processPartition directly with a
 * CountingHoodieStorage wrapper around a real local-FS storage, then asserts
 * the resulting openSeekable / seek call counts. Where possible, the same input
 * is re-run with a non-batching configuration (maxGapBytes = 0) to establish
 * a baseline and prove the reduction.
 *
 * Lives in the same package as BatchedBlobReader to access the package-private
 * RowAccessor / RowBuilder implicits.
 */
class TestBatchedBlobReaderIO extends HoodieClientTestBase {

  private val DEFAULT_LOOKAHEAD = 50

  /** Build rows by routing through Spark so the nested blob struct is encoded correctly. */
  private def buildRows(entries: Seq[(String, Long, Long)]): (Array[Row], StructType) = {
    val df = sparkSession.createDataFrame(entries).toDF("external_path", "offset", "length")
      .withColumn("data", blobStructCol("data", col("external_path"), col("offset"), col("length")))
      .select("data")
    (df.collect(), df.schema)
  }

  /** Run processPartition with the counting wrapper and drain the output iterator. */
  private def runWithCounting(
      rows: Array[Row],
      inputSchema: StructType,
      maxGap: Int,
      lookahead: Int = DEFAULT_LOOKAHEAD): CountingHoodieStorage = {
    val raw: HoodieStorage = HoodieStorageUtils.getStorage(storageConf)
    val counting = new CountingHoodieStorage(raw)
    try {
      val outputSchema = inputSchema.add(StructField(BatchedBlobReader.DATA_COL, BinaryType, nullable = true))
      val reader = new BatchedBlobReader(counting, maxGap, lookahead)
      import RowAccessor.rowAccessor
      import RowBuilder.rowBuilder
      val out = reader.processPartition[Row](rows.iterator, structColIdx = 0, outputSchema)
      // Force materialization (the underlying reads happen here).
      while (out.hasNext) {
        val r = out.next()
        // Touch the data column so any lazy work completes.
        r.getAs[Array[Byte]](BatchedBlobReader.DATA_COL)
      }
      counting
    } finally {
      counting.close()
    }
  }

  // --- Scenario (1): Multiple blobs in the same out-of-line file --------------

  @Test
  def testManyBlobsInSingleFileBatchable(): Unit = {
    val filePath = createTestFile(tempDir, "many-batch.bin", 50000)
    val entries = (0 until 20).map(i => (filePath, (i * 150).toLong, 100L))
    val (rows, schema) = buildRows(entries)

    val counters = runWithCounting(rows, schema, maxGap = 4096)
    assertEquals(1, counters.openSeekableCount.get, "20 blobs with 50-byte gaps should batch to one open")
    assertEquals(1, counters.seekCount.get, "...and one seek")
  }

  @Test
  def testManyBlobsInSingleFileNoBatching(): Unit = {
    val filePath = createTestFile(tempDir, "many-nobatch.bin", 50000)
    val entries = (0 until 20).map(i => (filePath, (i * 150).toLong, 100L))
    val (rows, schema) = buildRows(entries)

    val counters = runWithCounting(rows, schema, maxGap = 0)
    assertEquals(20, counters.openSeekableCount.get, "maxGap=0 with 50-byte gaps should produce one open per blob")
    assertEquals(20, counters.seekCount.get)
  }

  // --- Scenario (2): Contiguous out-of-line blobs (zero gap) ------------------

  @Test
  def testContiguousBlobsMergeWithZeroGap(): Unit = {
    val filePath = createTestFile(tempDir, "contig.bin", 1000)
    val entries = (0 until 6).map(i => (filePath, (i * 100).toLong, 100L))
    val (rows, schema) = buildRows(entries)

    // Zero gap, zero threshold -> gap (0) <= maxGap (0) -> all merge
    val counters = runWithCounting(rows, schema, maxGap = 0)
    assertEquals(1, counters.openSeekableCount.get, "Six contiguous blobs should merge into one read")
    assertEquals(1, counters.seekCount.get)
  }

  // --- Scenario (3): Out-of-line blobs with gaps (threshold-controlled) -------

  @Test
  def testSmallGapsWithinThresholdBatch(): Unit = {
    val filePath = createTestFile(tempDir, "small-gaps.bin", 10000)
    val entries = Seq(
      (filePath, 0L, 100L),
      (filePath, 120L, 100L),
      (filePath, 240L, 100L),
      (filePath, 360L, 100L),
      (filePath, 480L, 100L),
      (filePath, 600L, 100L))
    val (rows, schema) = buildRows(entries)

    val counters = runWithCounting(rows, schema, maxGap = 4096)
    assertEquals(1, counters.openSeekableCount.get)
    assertEquals(1, counters.seekCount.get)
  }

  @Test
  def testLargeGapsBeyondThresholdDoNotBatch(): Unit = {
    val filePath = createTestFile(tempDir, "large-gaps.bin", 50000)
    val entries = Seq(
      (filePath, 0L, 100L),
      (filePath, 8192L, 100L),
      (filePath, 16384L, 100L),
      (filePath, 24576L, 100L))
    val (rows, schema) = buildRows(entries)

    val counters = runWithCounting(rows, schema, maxGap = 1024)
    assertEquals(4, counters.openSeekableCount.get, "Gaps above threshold must not batch")
    assertEquals(4, counters.seekCount.get)
  }

  @Test
  def testThresholdBoundary(): Unit = {
    val filePath = createTestFile(tempDir, "boundary.bin", 5000)
    // Gap between consecutive blobs = exactly 1024 bytes:
    // [0,100), [1124,100), [2248,100), [3372,100)
    val entries = (0 until 4).map(i => (filePath, (i * 1124).toLong, 100L))
    val (rows, schema) = buildRows(entries)

    val inclusive = runWithCounting(rows, schema, maxGap = 1024)
    assertEquals(1, inclusive.openSeekableCount.get, "gap == maxGap is inclusive -> merge")

    val exclusive = runWithCounting(rows, schema, maxGap = 1023)
    assertEquals(4, exclusive.openSeekableCount.get, "gap > maxGap -> no merge")
  }

  @Test
  def testMixedSmallAndLargeGapsInOneFile(): Unit = {
    val filePath = createTestFile(tempDir, "mixed-gaps.bin", 20000)
    // Two batchable groups separated by a big gap.
    val entries = Seq(
      (filePath, 0L, 100L),
      (filePath, 200L, 100L),
      (filePath, 400L, 100L),
      // Large jump: gap = 10000 - 500 = 9500 > 4096
      (filePath, 10000L, 100L),
      (filePath, 10200L, 100L),
      (filePath, 10400L, 100L))
    val (rows, schema) = buildRows(entries)

    val counters = runWithCounting(rows, schema, maxGap = 4096)
    assertEquals(2, counters.openSeekableCount.get, "Two merged ranges -> two opens")
    assertEquals(2, counters.seekCount.get)
  }

  // --- Scenario (4): Blobs from multiple files in the same query --------------

  @Test
  def testBlobsFromMultipleFilesBatchedPerFile(): Unit = {
    val files = (0 until 3).map(i => createTestFile(tempDir, s"multi-$i.bin", 1000))
    val entries = files.flatMap { f =>
      (0 until 4).map(i => (f, (i * 100).toLong, 100L))
    }
    val (rows, schema) = buildRows(entries)

    val counters = runWithCounting(rows, schema, maxGap = 4096)
    assertEquals(3, counters.openSeekableCount.get, "One batched open per file")
    assertEquals(3, counters.seekCount.get)
  }

  @Test
  def testMultipleFilesWithMixedGapPatterns(): Unit = {
    val fileA = createTestFile(tempDir, "fileA.bin", 1000) // contiguous
    val fileB = createTestFile(tempDir, "fileB.bin", 2000) // small gaps
    val fileC = createTestFile(tempDir, "fileC.bin", 50000) // large gaps

    val entries =
      (0 until 4).map(i => (fileA, (i * 100).toLong, 100L)) ++         // contiguous -> 1 merged
      (0 until 4).map(i => (fileB, (i * 150).toLong, 100L)) ++         // 50-byte gaps -> 1 merged
      (0 until 4).map(i => (fileC, (i * 8192).toLong, 100L))           // 8KB gaps -> 4 ranges
    val (rows, schema) = buildRows(entries)

    val counters = runWithCounting(rows, schema, maxGap = 1024)
    assertEquals(6, counters.openSeekableCount.get, "1 (A) + 1 (B) + 4 (C) merged ranges")
    assertEquals(6, counters.seekCount.get)
  }

  @Test
  def testInterleavedFileOrderStillBatchesPerFile(): Unit = {
    val fileA = createTestFile(tempDir, "interA.bin", 1000)
    val fileB = createTestFile(tempDir, "interB.bin", 1000)
    val entries = Seq(
      (fileA, 0L, 100L),
      (fileB, 0L, 100L),
      (fileA, 100L, 100L),
      (fileB, 100L, 100L),
      (fileA, 200L, 100L),
      (fileB, 200L, 100L))
    val (rows, schema) = buildRows(entries)

    val counters = runWithCounting(rows, schema, maxGap = 4096)
    assertEquals(2, counters.openSeekableCount.get, "Interleaved input still groups per file")
    assertEquals(2, counters.seekCount.get)
  }
}
