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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hudi.blob.BatchedByteRangeReader
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Tests for BatchedByteRangeReader.
 *
 * These tests verify the batching behavior and effectiveness of the
 * BatchedByteRangeReader compared to non-batched approaches.
 */
class TestBatchedByteRangeReader extends HoodieClientTestBase {

  @Test
  def testBasicBatchedRead(): Unit = {
    val filePath = createTestFile(tempDir, "basic.bin", 10000)

    // Create input with struct column
    val inputDF = sparkSession.createDataFrame(Seq(
      (filePath, 0L, 100L),
      (filePath, 100L, 100L),
      (filePath, 200L, 100L)
    )).toDF("file_path", "position", "length")
      .withColumn("data", blobStructCol("data", col("file_path"), col("position"), col("length")))
      .select("data")

    // Read with batching
    val resultDF = BatchedByteRangeReader.readBatched(inputDF, storageConf)

    // Verify schema
    assertTrue(resultDF.columns.contains("data"))
    assertEquals(1, resultDF.columns.length) // data

    // Verify results
    val results = resultDF.collect()
    assertEquals(3, results.length)

    // Check data content
    results.zipWithIndex.foreach { case (row, i) =>
      val data = row.getAs[Array[Byte]]("data")
      assertEquals(100, data.length)

      // Verify content matches expected pattern
      assertBytesContent(data, expectedOffset = i * 100)
    }
  }

  @Test
  def testNoBatchingDifferentFiles(): Unit = {
    // Create different files
    val file1 = createTestFile(tempDir, "file1.bin", 5000)
    val file2 = createTestFile(tempDir, "file2.bin", 5000)
    val file3 = createTestFile(tempDir, "file3.bin", 5000)

    // Reads from different files (no batching possible)
    val inputDF = sparkSession.createDataFrame(Seq(
      (file1, 0L, 100L),
      (file2, 0L, 100L),
      (file3, 0L, 100L)
    )).toDF("file_path", "position", "length")
      .withColumn("data", blobStructCol("data", col("file_path"), col("position"), col("length")))
      .select("data")

    val resultDF = BatchedByteRangeReader.readBatched(inputDF, storageConf)

    val results = resultDF.collect()
    assertEquals(3, results.length)

    // Verify all reads succeeded
    results.foreach { row =>
      val data = row.getAs[Array[Byte]]("data")
      assertEquals(100, data.length)
    }
  }

  @Test
  def testGapThresholdSmallGaps(): Unit = {

    val filePath = createTestFile(tempDir, "small-gaps.bin", 10000)

    // Reads with small gaps (should batch with default threshold of 4KB)
    val inputDF = sparkSession.createDataFrame(Seq(
      (filePath, 0L, 100L),
      (filePath, 120L, 100L),    // 20 byte gap
      (filePath, 240L, 100L),    // 20 byte gap
      (filePath, 360L, 100L)     // 20 byte gap
    )).toDF("file_path", "position", "length")
      .withColumn("data", blobStructCol("data", col("file_path"), col("position"), col("length")))
      .select("data")

    // Use default maxGapBytes=4096 which should batch these
    val resultDF = BatchedByteRangeReader.readBatched(inputDF, storageConf, maxGapBytes = 4096)

    val results = resultDF.collect()
    assertEquals(4, results.length)

    results.foreach { row =>
      val data = row.getAs[Array[Byte]]("data")
      assertEquals(100, data.length)
    }
  }

  @Test
  def testGapThresholdLargeGaps(): Unit = {

    val filePath = createTestFile(tempDir, "large-gaps.bin", 50000)

    // Reads with large gaps (should NOT batch with small threshold)
    val inputDF = sparkSession.createDataFrame(Seq(
      (filePath, 0L, 100L),
      (filePath, 10000L, 100L),   // 9.9KB gap
      (filePath, 20000L, 100L),   // 9.9KB gap
      (filePath, 30000L, 100L)    // 9.9KB gap
    )).toDF("file_path", "position", "length")
      .withColumn("data", blobStructCol("data", col("file_path"), col("position"), col("length")))
      .select("data")

    // Use small maxGapBytes that won't batch these
    val resultDF = BatchedByteRangeReader.readBatched(inputDF, storageConf, maxGapBytes = 1000)

    val results = resultDF.collect()
    assertEquals(4, results.length)

    // Verify data correctness
    results.zipWithIndex.foreach { case (row, i) =>
      val data = row.getAs[Array[Byte]]("data")
      assertEquals(100, data.length)
    }
  }

  @Test
  def testPreserveInputOrder(): Unit = {

    val filePath = createTestFile(tempDir, "order.bin", 10000)

    // Create input in specific order with record IDs
    val inputDF = sparkSession.createDataFrame(Seq(
      (filePath, 0L, 100L, "rec1"),
      (filePath, 100L, 100L, "rec2"),
      (filePath, 200L, 100L, "rec3"),
      (filePath, 300L, 100L, "rec4")
    )).toDF("file_path", "position", "length", "record_id")
      .withColumn("data", blobStructCol("data", col("file_path"), col("position"), col("length")))
      .select("data", "record_id")

    val resultDF = BatchedByteRangeReader.readBatched(inputDF, storageConf)

    val results = resultDF.collect()
    assertEquals(4, results.length)

    // Verify order is preserved
    assertEquals("rec1", results(0).getAs[String]("record_id"))
    assertEquals("rec2", results(1).getAs[String]("record_id"))
    assertEquals("rec3", results(2).getAs[String]("record_id"))
    assertEquals("rec4", results(3).getAs[String]("record_id"))
  }

  @Test
  def testMixedScenario(): Unit = {

    val file1 = createTestFile(tempDir, "mixed1.bin", 10000)
    val file2 = createTestFile(tempDir, "mixed2.bin", 10000)

    // Mix of batchable and non-batchable reads
    val inputDF = sparkSession.createDataFrame(Seq(
      // Batchable group from file1
      (file1, 0L, 100L),
      (file1, 100L, 100L),
      (file1, 200L, 100L),
      // Single read from file2
      (file2, 0L, 100L),
      // Another batchable group from file1
      (file1, 300L, 100L),
      (file1, 400L, 100L),
      // Large gap in file1 (may not batch depending on threshold)
      (file1, 5000L, 100L)
    )).toDF("file_path", "position", "length")
      .withColumn("data", blobStructCol("data", col("file_path"), col("position"), col("length")))
      .select("data")

    val resultDF = BatchedByteRangeReader.readBatched(inputDF, storageConf)

    val results = resultDF.collect()
    assertEquals(7, results.length)

    // Verify all reads succeeded
    results.foreach { row =>
      val data = row.getAs[Array[Byte]]("data")
      assertEquals(100, data.length)
    }
  }

  @Test
  def testEmptyDataset(): Unit = {

    val inputDF = sparkSession.createDataFrame(Seq.empty[(String, Long, Int)])
      .toDF("file_path", "position", "length")
      .withColumn("data", blobStructCol("data", col("file_path"), col("position"), col("length")))
      .select("data")

    val resultDF = BatchedByteRangeReader.readBatched(inputDF, storageConf)

    assertEquals(0, resultDF.count())
  }

  @Test
  def testPreserveAdditionalColumns(): Unit = {

    val filePath = createTestFile(tempDir, "preserve-cols.bin", 5000)

    // Input with multiple additional columns
    val inputDF = sparkSession.createDataFrame(Seq(
      (filePath, 0L, 100L, "rec1", 42, true, 3.14),
      (filePath, 100L, 100L, "rec2", 43, false, 2.71)
      )).toDF("file_path", "position", "length", "record_id", "sequence", "flag", "value")
      .withColumn("data", blobStructCol("data", col("file_path"), col("position"), col("length")))
      .select("data", "record_id", "sequence", "flag", "value")

    val resultDF = BatchedByteRangeReader.readBatched(inputDF, storageConf)

    // Verify all columns are preserved
    assertColumnsExist(resultDF, "data", "record_id", "sequence", "flag", "value")

    val results = resultDF.collect()
    assertEquals(2, results.length)

    // Verify data integrity
    assertEquals("rec1", results(0).getAs[String]("record_id"))
    assertEquals(42, results(0).getAs[Int]("sequence"))
    assertEquals(true, results(0).getAs[Boolean]("flag"))
    assertEquals(3.14, results(0).getAs[Double]("value"), 0.001)

    assertEquals("rec2", results(1).getAs[String]("record_id"))
    assertEquals(43, results(1).getAs[Int]("sequence"))
    assertEquals(false, results(1).getAs[Boolean]("flag"))
    assertEquals(2.71, results(1).getAs[Double]("value"), 0.001)
  }

  @Test
  def testExplicitColumnNameWithMultipleBlobColumns(): Unit = {
    // Test that explicit column name resolves the correct column
    // when multiple blob columns exist in the schema

    val file1 = createTestFile(tempDir, "blob1.bin", 5000)
    val file2 = createTestFile(tempDir, "blob2.bin", 5000)

    // Create DataFrame with two blob columns
    val inputDF = sparkSession.createDataFrame(Seq(
      (1, file1, 0L, 100L, file2, 0L, 50L),
      (2, file1, 100L, 100L, file2, 50L, 50L)
    )).toDF("id", "path1", "position1", "len1", "path2", "position2", "len2")
      .withColumn("blob1", blobStructCol("blob1", col("path1"), col("position1"), col("len1")))
      .withColumn("blob2", blobStructCol("blob2", col("path2"), col("position2"), col("len2")))
      .select("id", "blob1", "blob2")

    // Resolve blob1 explicitly
    val result1 = BatchedByteRangeReader.readBatched(
      inputDF,
      storageConf,
      columnName = Some("blob1")
    )

    val rows1 = result1.collect()
    assertEquals(2, rows1.length)

    // Verify blob1 data was read (100 bytes)
    rows1.foreach { row =>
      val data = row.getAs[Array[Byte]]("blob1")
      assertEquals(100, data.length)
    }

    // Resolve blob2 explicitly
    val result2 = BatchedByteRangeReader.readBatched(
      inputDF,
      storageConf,
      columnName = Some("blob2")
    )

    val rows2 = result2.collect()
    assertEquals(2, rows2.length)

    // Verify blob2 data was read (50 bytes)
    rows2.foreach { row =>
      val data = row.getAs[Array[Byte]]("blob2")
      assertEquals(50, data.length)
    }
  }

  @Test
  def testExplicitColumnNameWithoutBlobMetadata(): Unit = {
    // Test that explicit column name works even when column doesn't have hudi_blob metadata
    // This allows the SQL plan rule to directly specify which column to resolve

    val filePath = createTestFile(tempDir, "no-metadata.bin", 5000)

    // Create DataFrame with struct column but NO blob metadata
    val inputDF = sparkSession.createDataFrame(Seq(
      (filePath, 0L, 100L),
      (filePath, 100L, 100L)
    )).toDF("file_path", "position", "length")
      .withColumn("file_info", struct(
        lit("out_of_line").as("storage_type"),
        lit(null).cast("binary").as("bytes"),
        struct(
          col("file_path").as("file"),
          col("position").as("position"),
          col("length").as("length"),
          lit(false).as("managed")
        ).as("reference")
      ))
      .select("file_info")

    // Should work when column name is explicitly provided
    val resultDF = BatchedByteRangeReader.readBatched(
      inputDF,
      storageConf,
      columnName = Some("file_info")
    )

    val results = resultDF.collect()
    assertEquals(2, results.length)

    results.foreach { row =>
      val data = row.getAs[Array[Byte]]("file_info")
      assertEquals(100, data.length)
    }
  }

  @Test
  def testFallbackToMetadataWhenNoColumnNameProvided(): Unit = {
    // Test that when no explicit column name is provided,
    // it falls back to searching for hudi_blob=true metadata

    val filePath = createTestFile(tempDir, "fallback.bin", 5000)

    // Create DataFrame with blob metadata (the traditional way)
    val inputDF = sparkSession.createDataFrame(Seq(
      (filePath, 0L, 100L),
      (filePath, 100L, 100L)
    )).toDF("file_path", "position", "length")
      .withColumn("data", blobStructCol("data", col("file_path"), col("position"), col("length")))
      .select("data")

    // Should work without explicit column name (uses metadata)
    val resultDF = BatchedByteRangeReader.readBatched(
      inputDF,
      storageConf
      // Note: columnName parameter is NOT provided
    )

    val results = resultDF.collect()
    assertEquals(2, results.length)

    results.foreach { row =>
      val data = row.getAs[Array[Byte]]("data")
      assertEquals(100, data.length)
    }
  }
}
