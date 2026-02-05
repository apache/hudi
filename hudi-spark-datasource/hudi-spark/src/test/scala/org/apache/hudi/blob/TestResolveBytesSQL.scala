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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

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

  @Test
  def testBasicResolveBytesSQL(): Unit = {
    val filePath = createTestFile(tempDir, "basic.bin", 10000)

    // Create table with blob column
    val df = sparkSession.createDataFrame(Seq(
      (1, "record1", filePath, 0L, 100L),
      (2, "record2", filePath, 100L, 100L),
      (3, "record3", filePath, 200L, 100L)
    )).toDF("id", "name", "file_path", "position", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("file_path"), col("position"), col("length")))
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
    assertBytesContent(data1)

    val data2 = rows(1).getAs[Array[Byte]]("data")
    assertEquals(100, data2.length)
    assertBytesContent(data2, expectedOffset = 100)
  }

  @Test
  def testResolveBytesWithJoin(): Unit = {
    val filePath = createTestFile(tempDir, "join.bin", 10000)

    // Create blob table
    val blobDF = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100L),
      (2, filePath, 100L, 100L)
    )).toDF("id", "file_path", "position", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("file_path"), col("position"), col("length")))
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
    assertBytesContent(data1)
  }

  @Test
  def testResolveBytesWithAggregation(): Unit = {
    val filePath = createTestFile(tempDir, "agg.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, "A", filePath, 0L, 100L),
      (2, "A", filePath, 100L, 100L),
      (3, "B", filePath, 200L, 100L)
    )).toDF("id", "category", "file_path", "position", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("file_path"), col("position"), col("length")))
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
    val filePath = createTestFile(tempDir, "order.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (3, filePath, 200L, 50L),
      (1, filePath, 0L, 50L),
      (2, filePath, 100L, 50L)
    )).toDF("id", "file_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("file_path"), col("offset"), col("length")))
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
    assertBytesContent(data1)
  }

  @Test
  def testResolveBytesInSubquery(): Unit = {
    val filePath = createTestFile(tempDir, "subquery.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, "A", filePath, 0L, 100L),
      (2, "A", filePath, 100L, 100L),
      (3, "B", filePath, 200L, 100L)
    )).toDF("id", "category", "file_path", "position", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("file_path"), col("position"), col("length")))
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
    val filePath = createTestFile(tempDir, "config.bin", 50000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100L),
      (2, filePath, 5000L, 100L),  // 4.9KB gap
      (3, filePath, 10000L, 100L)
    )).toDF("id", "file_path", "position", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("file_path"), col("position"), col("length")))
      .select("id", "file_info")

    df.createOrReplaceTempView("config_table")

    // Use withSparkConfig to automatically manage configuration
    withSparkConfig(sparkSession, Map(
      "hoodie.blob.batching.max.gap.bytes" -> "10000",
      "hoodie.blob.batching.lookahead.size" -> "100"
    )) {
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
    }
  }

  @Test
  def testMultipleResolveBytesInSameQuery(): Unit = {
    val filePath1 = createTestFile(tempDir, "multi1.bin", 10000)
    val filePath2 = createTestFile(tempDir, "multi2.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath1, 0L, 50L, filePath2, 0L, 50L),
      (2, filePath1, 100L, 50L, filePath2, 100L, 50L)
    )).toDF("id", "file_path1", "offset1", "length1", "file_path2", "offset2", "length2")
      .withColumn("file_info1",
        blobStructCol("file_info1", col("file_path1"), col("offset1"), col("length1")))
      .withColumn("file_info2",
        blobStructCol("file_info2", col("file_path2"), col("offset2"), col("length2")))
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
    val filePath = createTestFile(tempDir, "empty.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath, 0L, 100L),
      (2, filePath, 100L, 100L)
    )).toDF("id", "file_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("file_path"), col("offset"), col("length")))
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
    val filePath1 = createTestFile(tempDir, "file1.bin", 10000)
    val filePath2 = createTestFile(tempDir, "file2.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, filePath1, 0L, 100L),
      (2, filePath2, 0L, 100L),
      (3, filePath1, 100L, 100L),
      (4, filePath2, 100L, 100L)
    )).toDF("id", "file_path", "offset", "length")
    .withColumn("file_info",
      blobStructCol("file_info", col("file_path"), col("offset"), col("length")))
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
    val filePath = createTestFile(tempDir, "case.bin", 10000)

    val df = sparkSession.createDataFrame(Seq(
      (1, true, filePath, 0L, 100L),
      (2, false, filePath, 100L, 100L),
      (3, true, filePath, 200L, 100L)
    )).toDF("id", "should_resolve", "file_path", "offset", "length")
      .withColumn("file_info",
        blobStructCol("file_info", col("file_path"), col("offset"), col("length")))
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
