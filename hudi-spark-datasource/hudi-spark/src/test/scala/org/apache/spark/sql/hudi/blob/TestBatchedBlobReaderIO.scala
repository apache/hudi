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
 * the resulting openSeekable / seek call counts.
 *
 * Scope: only the merge-result-to-physical-I/O mapping is exercised here. The
 * gap/threshold merging algorithm itself is covered by TestBatchedBlobReaderMerge.
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
      while (out.hasNext) {
        val r = out.next()
        r.getAs[Array[Byte]](BatchedBlobReader.DATA_COL)
      }
      counting
    } finally {
      counting.close()
    }
  }

  /**
   * End-to-end proof that batching reduces I/O: the same input collapses from
   * N opens (maxGap=0, gaps exceed threshold) to a single open (maxGap=4096).
   */
  @Test
  def testBatchingReducesIOForSingleFile(): Unit = {
    val filePath = createTestFile(tempDir, "single-file.bin", 50000)
    val entries = (0 until 20).map(i => (filePath, (i * 150).toLong, 100L))
    val (rows, schema) = buildRows(entries)

    val batched = runWithCounting(rows, schema, maxGap = 4096)
    assertEquals(1, batched.openSeekableCount.get, "20 blobs with 50-byte gaps should batch to one open")
    assertEquals(1, batched.seekCount.get)

    val nonBatched = runWithCounting(rows, schema, maxGap = 0)
    assertEquals(20, nonBatched.openSeekableCount.get, "maxGap=0 with 50-byte gaps should produce one open per blob")
    assertEquals(20, nonBatched.seekCount.get)
  }

  /**
   * Mixed small/large gaps in one file produce exactly one I/O per merged group.
   * Threshold-boundary correctness is covered by TestBatchedBlobReaderMerge.
   */
  @Test
  def testMixedGapsProduceOneIOPerMergedGroup(): Unit = {
    val filePath = createTestFile(tempDir, "mixed-gaps.bin", 20000)
    // Two batchable groups separated by a 9500-byte gap (> maxGap=4096).
    val entries = Seq(
      (filePath, 0L, 100L),
      (filePath, 200L, 100L),
      (filePath, 400L, 100L),
      (filePath, 10000L, 100L),
      (filePath, 10200L, 100L),
      (filePath, 10400L, 100L))
    val (rows, schema) = buildRows(entries)

    val counters = runWithCounting(rows, schema, maxGap = 4096)
    assertEquals(2, counters.openSeekableCount.get, "Two merged ranges -> two opens")
    assertEquals(2, counters.seekCount.get)
  }

  /**
   * Multi-file routing at the I/O level: interleaved input + mixed gap patterns
   * across three files should still produce per-file batched opens.
   */
  @Test
  def testMultiFileBatchingAtIOLevel(): Unit = {
    val fileA = createTestFile(tempDir, "fileA.bin", 1000)   // contiguous
    val fileB = createTestFile(tempDir, "fileB.bin", 2000)   // small gaps within threshold
    val fileC = createTestFile(tempDir, "fileC.bin", 50000)  // large gaps above threshold

    // Interleaved order so we also confirm grouping isn't order-dependent at the I/O layer.
    val entries =
      (0 until 4).flatMap { i =>
        Seq(
          (fileA, (i * 100).toLong, 100L),       // contiguous -> 1 merged
          (fileB, (i * 150).toLong, 100L),       // 50-byte gaps -> 1 merged
          (fileC, (i * 8192).toLong, 100L))      // 8KB gaps -> 4 ranges
      }
    val (rows, schema) = buildRows(entries)

    val counters = runWithCounting(rows, schema, maxGap = 1024)
    assertEquals(6, counters.openSeekableCount.get, "1 (A) + 1 (B) + 4 (C) merged ranges -> 6 opens")
    assertEquals(6, counters.seekCount.get)
  }
}
