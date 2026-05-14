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

package org.apache.hudi.blob

import org.apache.hudi.blob.BlobTestHelpers._
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * SQL-driven correctness tests for read_blob() batching scenarios.
 *
 * Complements:
 *   - TestBatchedBlobReaderMerge: unit-level coverage of the merge algorithm.
 *   - TestReadBlobSQL: general SQL surface (joins, subqueries, config plumbing,
 *     null handling, error paths).
 *   - TestBatchedBlobReader: byte-level correctness via the readBatched API.
 *
 * Scope: exercise the SQL path end-to-end with batching configurations chosen
 * to drive specific merge behaviors (high-ratio merging, threshold boundary,
 * per-file routing), and assert that the returned bytes match what the input
 * specified — i.e. the query succeeds and batching does not corrupt output.
 */
class TestReadBlobBatching extends HoodieClientTestBase {

  /**
   * Build a temp view named `viewName` from (id, file, offset, length) tuples
   * with a blob struct column `file_info`.
   */
  private def registerBlobView(
      viewName: String,
      entries: Seq[(Int, String, Long, Long)]): Unit = {
    sparkSession.createDataFrame(entries)
      .toDF("id", "external_path", "offset", "length")
      .withColumn("file_info", blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "external_path", "offset", "file_info")
      .createOrReplaceTempView(viewName)
  }

  /**
   * Run SELECT id, external_path, offset, read_blob(file_info) and assert each
   * returned blob equals the deterministic pattern at its recorded offset.
   */
  private def assertBlobBytesMatchExpected(rows: Array[Row], expectedLength: Int): Unit = {
    rows.foreach { row =>
      val offset = row.getAs[Long]("offset")
      val data = row.getAs[Array[Byte]]("data")
      assertEquals(expectedLength, data.length, s"length mismatch at offset $offset")
      assertBytesContent(data, expectedOffset = offset.toInt)
    }
  }

  /**
   * Scenario (1) — many blobs in a single file batch into one read.
   *
   * 20 small reads with 50-byte gaps under maxGap=4096 must all merge into a
   * single underlying range. Validates that high-ratio batching returns the
   * correct bytes for every row.
   */
  @Test
  def testManyBlobsInSingleFileBatched(): Unit = {
    val filePath = createTestFile(tempDir, "many.bin", 50000)
    val entries = (0 until 20).map(i => (i, filePath, (i * 150).toLong, 100L))
    registerBlobView("many_blobs_view", entries)

    withSparkConfig(sparkSession, Map(
      "hoodie.blob.batching.max.gap.bytes" -> "4096"
    )) {
      val rows = sparkSession.sql(
        """SELECT id, external_path, offset, read_blob(file_info) AS data
          |FROM many_blobs_view
          |ORDER BY id""".stripMargin).collect()
      assertEquals(20, rows.length)
      assertBlobBytesMatchExpected(rows, expectedLength = 100)
    }
  }

  /**
   * Scenario (2) — mixed small and large gaps in one file.
   *
   * Two batchable groups separated by a 9500-byte gap (above maxGap=4096) must
   * produce correct bytes for all six rows regardless of the gap-driven split
   * inside the reader.
   */
  @Test
  def testMixedGapsInSingleFile(): Unit = {
    val filePath = createTestFile(tempDir, "mixed.bin", 20000)
    val entries = Seq(
      (1, filePath, 0L, 100L),
      (2, filePath, 200L, 100L),
      (3, filePath, 400L, 100L),
      (4, filePath, 10000L, 100L),
      (5, filePath, 10200L, 100L),
      (6, filePath, 10400L, 100L))
    registerBlobView("mixed_gaps_view", entries)

    withSparkConfig(sparkSession, Map(
      "hoodie.blob.batching.max.gap.bytes" -> "4096"
    )) {
      val rows = sparkSession.sql(
        """SELECT id, external_path, offset, read_blob(file_info) AS data
          |FROM mixed_gaps_view
          |ORDER BY id""".stripMargin).collect()
      assertEquals(6, rows.length)
      assertBlobBytesMatchExpected(rows, expectedLength = 100)
    }
  }

  /**
   * Scenario (3) — threshold boundary at SQL level.
   *
   * Four reads with gaps of exactly maxGap (1024). The merge algorithm treats
   * gap == maxGap as inclusive (covered by TestBatchedBlobReaderMerge); this
   * test confirms the SQL surface returns correct bytes at that boundary.
   */
  @Test
  def testThresholdBoundaryInclusiveMerge(): Unit = {
    val filePath = createTestFile(tempDir, "boundary.bin", 5000)
    // Gap between consecutive reads = 1024 bytes (1124 - 100, etc.).
    val entries = (0 until 4).map(i => (i, filePath, (i * 1124).toLong, 100L))
    registerBlobView("boundary_view", entries)

    withSparkConfig(sparkSession, Map(
      "hoodie.blob.batching.max.gap.bytes" -> "1024"
    )) {
      val rows = sparkSession.sql(
        """SELECT id, external_path, offset, read_blob(file_info) AS data
          |FROM boundary_view
          |ORDER BY id""".stripMargin).collect()
      assertEquals(4, rows.length)
      assertBlobBytesMatchExpected(rows, expectedLength = 100)
    }
  }

  /**
   * Scenario (4) — multi-file with interleaved input + mixed gap patterns.
   *
   * Three files with different access patterns (contiguous, small gaps within
   * threshold, large gaps above threshold) queried in interleaved order.
   * Confirms per-file routing through the SQL planner returns correct bytes
   * regardless of input ordering.
   */
  @Test
  def testMultiFileInterleavedWithMixedGaps(): Unit = {
    val fileA = createTestFile(tempDir, "fileA.bin", 1000)   // contiguous
    val fileB = createTestFile(tempDir, "fileB.bin", 2000)   // 50-byte gaps
    val fileC = createTestFile(tempDir, "fileC.bin", 50000)  // 8KB gaps

    val entries = (0 until 4).flatMap { i =>
      Seq(
        (i * 3,     fileA, (i * 100).toLong,  100L),
        (i * 3 + 1, fileB, (i * 150).toLong,  100L),
        (i * 3 + 2, fileC, (i * 8192).toLong, 100L))
    }
    registerBlobView("multi_file_view", entries)

    withSparkConfig(sparkSession, Map(
      "hoodie.blob.batching.max.gap.bytes" -> "1024"
    )) {
      val rows = sparkSession.sql(
        """SELECT id, external_path, offset, read_blob(file_info) AS data
          |FROM multi_file_view
          |ORDER BY id""".stripMargin).collect()
      assertEquals(12, rows.length)
      assertBlobBytesMatchExpected(rows, expectedLength = 100)
    }
  }
}
