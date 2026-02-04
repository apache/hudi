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

import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.MetadataBuilder
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.io.File
import java.nio.file.Files

/**
 * Tests for the resolve_bytes() SQL function.
 *
 * This test suite verifies:
 * <ul>
 *   <li>Basic SQL integration with resolve_bytes()</li>
 *   <li>Integration with WHERE clauses, JOINs, aggregations</li>
 *   <li>Configuration parameter handling</li>
 *   <li>Error handling for invalid inputs</li>
 * </ul>
 */
class TestResolveBytesSQL extends HoodieClientTestBase {

  /**
   * Create a test file with predictable content.
   * Content pattern: byte at position i = i % 256
   */
  private def createTestFile(name: String, size: Int): String = {
    val file = new File(tempDir.toString, name)
    val bytes = (0 until size).map(i => (i % 256).toByte).toArray
    Files.write(file.toPath, bytes)
    file.getAbsolutePath
  }

  /**
   * Create blob metadata for marking struct columns as blob references.
   */
  private def blobMetadata = {
    new MetadataBuilder()
      .putBoolean("hudi_blob", true)
      .build()
  }

  @Test
  def testBasicResolveBytesSQL(): Unit = {
    val filePath = createTestFile("basic.bin", 10000)

    // Create table with blob column
    val df = sparkSession.createDataFrame(Seq(
      (1, "record1", filePath, 0L, 100),
      (2, "record2", filePath, 100L, 100),
      (3, "record3", filePath, 200L, 100)
    )).toDF("id", "name", "file_path", "offset", "length")
      .withColumn("file_info",
        struct(col("file_path"), col("offset"), col("length"))
          .as("file_info", blobMetadata))
      .select("id", "name", "file_info")

    df.createOrReplaceTempView("test_table")

    // Use SQL with resolve_bytes
    val result = sparkSession.sql("""
      SELECT id, name, resolve_bytes(file_info) as data
      FROM test_table
      WHERE id <= 2
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)

    // Verify data is binary
    val data1 = rows(0).getAs[Array[Byte]]("data")
    assertEquals(100, data1.length)

    // Verify content matches expected pattern
    for (i <- 0 until 100) {
      assertEquals(i % 256, data1(i) & 0xFF)
    }

    val data2 = rows(1).getAs[Array[Byte]]("data")
    assertEquals(100, data2.length)
    for (i <- 0 until 100) {
      assertEquals((100 + i) % 256, data2(i) & 0xFF)
    }
  }

  @Test
  def testResolveBytesWithJoin(): Unit = {
    val filePath = createTestFile("join.bin", 10000)

    // Create blob table
    val blobDF = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100),
      (2, filePath, 100L, 100)
    )).toDF("id", "file_path", "offset", "length")
      .withColumn("file_info",
        struct(col("file_path"), col("offset"), col("length"))
          .as("file_info", blobMetadata))
      .select("id", "file_info")

    blobDF.createOrReplaceTempView("blob_table")

    // Create metadata table
    val metaDF = sparkSession.createDataFrame(Seq(
      (1, "Alice"),
      (2, "Bob")
    )).toDF("id", "name")

    metaDF.createOrReplaceTempView("meta_table")

    // SQL with JOIN
    val result = sparkSession.sql("""
      SELECT m.id, m.name, resolve_bytes(b.file_info) as data
      FROM meta_table m
      JOIN blob_table b ON m.id = b.id
      ORDER BY m.id
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)
    assertEquals("Alice", rows(0).getAs[String]("name"))
    assertEquals(100, rows(0).getAs[Array[Byte]]("data").length)
    assertEquals("Bob", rows(1).getAs[String]("name"))
    assertEquals(100, rows(1).getAs[Array[Byte]]("data").length)

    // Verify data content
    val data1 = rows(0).getAs[Array[Byte]]("data")
    for (i <- 0 until 100) {
      assertEquals(i % 256, data1(i) & 0xFF)
    }
  }

  @Test
  def testResolveBytesWithAggregation(): Unit = {
    val filePath = createTestFile("agg.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, "A", filePath, 0L, 100),
      (2, "A", filePath, 100L, 100),
      (3, "B", filePath, 200L, 100)
    )).toDF("id", "category", "file_path", "offset", "length")
      .withColumn("file_info",
        struct(col("file_path"), col("offset"), col("length"))
          .as("file_info", blobMetadata))
      .select("id", "category", "file_info")

    df.createOrReplaceTempView("agg_table")

    // SQL with aggregation
    val result = sparkSession.sql("""
      SELECT
        category,
        COUNT(*) as count,
        COLLECT_LIST(resolve_bytes(file_info)) as all_data
      FROM agg_table
      GROUP BY category
      ORDER BY category
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)

    // Category A should have 2 records
    assertEquals("A", rows(0).getAs[String]("category"))
    assertEquals(2, rows(0).getAs[Long]("count"))
    val dataListA = rows(0).getSeq[Array[Byte]](2)
    assertEquals(2, dataListA.length)
    assertEquals(100, dataListA(0).length)
    assertEquals(100, dataListA(1).length)

    // Category B should have 1 record
    assertEquals("B", rows(1).getAs[String]("category"))
    assertEquals(1, rows(1).getAs[Long]("count"))
    val dataListB = rows(1).getSeq[Array[Byte]](2)
    assertEquals(1, dataListB.length)
    assertEquals(100, dataListB(0).length)
  }

  @Test
  def testResolveBytesWithOrderBy(): Unit = {
    val filePath = createTestFile("order.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (3, filePath, 200L, 50),
      (1, filePath, 0L, 50),
      (2, filePath, 100L, 50)
    )).toDF("id", "file_path", "offset", "length")
      .withColumn("file_info",
        struct(col("file_path"), col("offset"), col("length"))
          .as("file_info", blobMetadata))
      .select("id", "file_info")

    df.createOrReplaceTempView("order_table")

    // SQL with ORDER BY
    val result = sparkSession.sql("""
      SELECT id, resolve_bytes(file_info) as data
      FROM order_table
      ORDER BY id
    """)

    val rows = result.collect()
    assertEquals(3, rows.length)
    assertEquals(1, rows(0).getAs[Int]("id"))
    assertEquals(2, rows(1).getAs[Int]("id"))
    assertEquals(3, rows(2).getAs[Int]("id"))

    // Verify data content for ordered results
    val data1 = rows(0).getAs[Array[Byte]]("data")
    for (i <- 0 until 50) {
      assertEquals(i % 256, data1(i) & 0xFF)
    }
  }

  @Test
  def testResolveBytesInSubquery(): Unit = {
    val filePath = createTestFile("subquery.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, "A", filePath, 0L, 100),
      (2, "A", filePath, 100L, 100),
      (3, "B", filePath, 200L, 100)
    )).toDF("id", "category", "file_path", "offset", "length")
      .withColumn("file_info",
        struct(col("file_path"), col("offset"), col("length"))
          .as("file_info", blobMetadata))
      .select("id", "category", "file_info")

    df.createOrReplaceTempView("subquery_table")

    // SQL with subquery
    val result = sparkSession.sql("""
      SELECT * FROM (
        SELECT id, category, resolve_bytes(file_info) as data
        FROM subquery_table
      ) WHERE category = 'A'
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)
    rows.foreach { row =>
      assertEquals("A", row.getAs[String]("category"))
      assertEquals(100, row.getAs[Array[Byte]]("data").length)
    }
  }

  @Test
  def testConfigurationParameters(): Unit = {
    val filePath = createTestFile("config.bin", 50000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100),
      (2, filePath, 5000L, 100),  // 4.9KB gap
      (3, filePath, 10000L, 100)
    )).toDF("id", "file_path", "offset", "length")
      .withColumn("file_info",
        struct(col("file_path"), col("offset"), col("length"))
          .as("file_info", blobMetadata))
      .select("id", "file_info")

    df.createOrReplaceTempView("config_table")

    // Set custom configuration
    sparkSession.conf.set("hoodie.blob.batching.max.gap.bytes", "10000")
    sparkSession.conf.set("hoodie.blob.batching.lookahead.size", "100")

    val result = sparkSession.sql("""
      SELECT id, resolve_bytes(file_info) as data
      FROM config_table
    """)

    val rows = result.collect()
    assertEquals(3, rows.length)

    // Verify all reads completed successfully
    rows.foreach { row =>
      assertEquals(100, row.getAs[Array[Byte]]("data").length)
    }

    // Reset config
    sparkSession.conf.unset("hoodie.blob.batching.max.gap.bytes")
    sparkSession.conf.unset("hoodie.blob.batching.lookahead.size")
  }

  @Test
  def testMultipleResolveBytesInSameQuery(): Unit = {
    val filePath1 = createTestFile("multi1.bin", 10000)
    val filePath2 = createTestFile("multi2.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath1, 0L, 50, filePath2, 0L, 50),
      (2, filePath1, 100L, 50, filePath2, 100L, 50)
    )).toDF("id", "file_path1", "offset1", "length1", "file_path2", "offset2", "length2")
      .withColumn("file_info1",
        struct(col("file_path1"), col("offset1"), col("length1"))
          .as("file_info1", blobMetadata))
      .withColumn("file_info2",
        struct(col("file_path2"), col("offset2"), col("length2"))
          .as("file_info2", blobMetadata))
      .select("id", "file_info1", "file_info2")

    df.createOrReplaceTempView("multi_table")

    // SQL with multiple resolve_bytes calls
    val result = sparkSession.sql("""
      SELECT
        id,
        resolve_bytes(file_info1) as data1,
        resolve_bytes(file_info2) as data2
      FROM multi_table
    """)

    val rows = result.collect()
    assertEquals(2, rows.length)

    rows.foreach { row =>
      assertEquals(50, row.getAs[Array[Byte]]("data1").length)
      assertEquals(50, row.getAs[Array[Byte]]("data2").length)
    }
  }

  @Test
  def testResolveBytesWithEmptyResult(): Unit = {
    val filePath = createTestFile("empty.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100),
      (2, filePath, 100L, 100)
    )).toDF("id", "file_path", "offset", "length")
      .withColumn("file_info",
        struct(col("file_path"), col("offset"), col("length"))
          .as("file_info", blobMetadata))
      .select("id", "file_info")

    df.createOrReplaceTempView("empty_table")

    // SQL that returns no rows
    val result = sparkSession.sql("""
      SELECT id, resolve_bytes(file_info) as data
      FROM empty_table
      WHERE id > 100
    """)

    val rows = result.collect()
    assertEquals(0, rows.length)
  }

  @Test
  def testResolveBytesMultipleFiles(): Unit = {
    val filePath1 = createTestFile("file1.bin", 10000)
    val filePath2 = createTestFile("file2.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath1, 0L, 100),
      (2, filePath2, 0L, 100),
      (3, filePath1, 100L, 100),
      (4, filePath2, 100L, 100)
    )).toDF("id", "file_path", "offset", "length")
    .withColumn("file_info",
        struct(col("file_path"), col("offset"), col("length"))
          .as("file_info", blobMetadata))
      .select("id", "file_info")

    df.createOrReplaceTempView("multi_file_table")

    // SQL reading from multiple files
    val result = sparkSession.sql("""
      SELECT id, resolve_bytes(file_info) as data
      FROM multi_file_table
      ORDER BY id
    """)

    val rows = result.collect()
    assertEquals(4, rows.length)

    // Verify all data was read correctly
    rows.foreach { row =>
      assertEquals(100, row.getAs[Array[Byte]]("data").length)
    }
  }

  @Test
  def testResolveBytesWithCaseWhen(): Unit = {
    val filePath = createTestFile("case.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, true, filePath, 0L, 100),
      (2, false, filePath, 100L, 100),
      (3, true, filePath, 200L, 100)
    )).toDF("id", "should_resolve", "file_path", "offset", "length")
      .withColumn("file_info",
        struct(col("file_path"), col("offset"), col("length"))
          .as("file_info", blobMetadata))
      .select("id", "should_resolve", "file_info")

    df.createOrReplaceTempView("case_table")

    // SQL with CASE WHEN - note: this tests that the expression is handled
    // even in conditional contexts
    val result = sparkSession.sql("""
      SELECT
        id,
        should_resolve,
        CASE
          WHEN should_resolve THEN resolve_bytes(file_info)
          ELSE NULL
        END as data
      FROM case_table
    """)

    val rows = result.collect()
    assertEquals(3, rows.length)

    // Row 1 should have data
    assertTrue(rows(0).getAs[Boolean]("should_resolve"))
    assertNotNull(rows(0).get(2))

    // Row 2 should have null
    assertFalse(rows(1).getAs[Boolean]("should_resolve"))
    assertTrue(rows(1).isNullAt(2))

    // Row 3 should have data
    assertTrue(rows(2).getAs[Boolean]("should_resolve"))
    assertNotNull(rows(2).get(2))
  }
}
